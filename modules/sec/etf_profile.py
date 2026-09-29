"""
Finds the actively managed equity funds among the SEC active ETFs (sec_active_etf) from their FMP
data, and places them - with a profile of what they hold - in the test provider tables
(test_provider, test_provider_etf, test_provider_etf_holding), parallel to the scraped provider
tables and disabled until an admin approves them.

Per fund, two FMP calls: etf/info (asset class, provider, description, website, AUM, NAV,
inception, sector weights) and etf/holdings. The reference for a fund's stocks is the index funds'
stored snapshot (modules/ticker/index_funds.py - universe_etf: VTI for the US, VEA + VWO for
International; downloaded on Wednesdays): a stock the US fund holds is US, one the international
funds hold is international, and each carries its company's float cap. A stock is large when its
float cap reaches its market's 70% breakpoint (the companies making up the top 70% of the
market's float cap), small below the 90% one, mid in between - Morningstar-style.

A fund's strategy (classify): leveraged_inverse (its N-CEN says it seeks a multiple / inverse of
an index, FMP's asset class, or its name), fund_of_funds (its N-CEN - left out), buffer and
option_income (its name - FMP files many under equity), then FMP's asset class; equity needs an
equity (or no) asset class and at least EQUITY_MIN_WEIGHT of the fund in stock lines. Only equity
funds go in the test tables, profiled by region (US / International / Global), cap size (large /
mid / small / smid / all), value / growth (large-cap funds, from our tickers' style_type) and FMP's
sector weights. Their holdings are stored once. Every checked fund's strategy is kept in
sec_etf_classification, so it's checked again only after PROFILE_REFRESH_DAYS or a newer filing.
"""
import log
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from collections import Counter, defaultdict
from modules.core import api_stocks
from modules.object import batch_run, sec_active_etf, sec_etf_classification, test_provider, test_provider_etf, test_provider_etf_holding
from modules.object.batch_run import BatchRun
from modules.object.sec_active_etf import SecActiveEtf
from modules.object.sec_etf_classification import SecEtfClassification
from modules.object.test_provider_etf import TestProviderEtf, NOT_EQUITY_REASON, SEC_GONE_REASON
from modules.object.test_provider_etf_holding import TestProviderEtfHolding
from modules.sec import ncen
from modules.ticker import index_funds

PROFILE_REFRESH_DAYS = 28   # a fund's profile is checked again after this long (or after a newer filing)
FETCH_WORKERS = 4           # funds fetched in parallel, all under api_stocks' 200-calls-a-minute throttle
EQUITY_MIN_WEIGHT = 0.8     # share of the fund in stock lines an equity fund needs
MIN_COVERAGE = 0.5          # share of the fund matched to the index funds' stocks needed for region and size
US_MIN = 0.8                # US share of the matched stocks for 'US'; at most 1 - US_MIN for 'International'
LARGE_COVERAGE = 0.70       # a stock is large at or above its market's breakpoint at this coverage...
MID_COVERAGE = 0.90         # ... mid down to this one, small below (Morningstar-style)
LARGE_MIN = 0.7             # cap size: large share for 'large'
SMALL_MIN = 0.5             # ... small share for 'small'
MID_MIN = 0.5               # ... mid share for 'mid'
SMID_MIN = 0.7              # ... mid + small share for 'smid'; otherwise 'all'
STYLE_MIN = 0.6             # value (growth) share of the style-classified weight for 'value' ('growth')
STYLE_MIN_COVERAGE = 0.5    # style-classified share of the stock weight needed for value / growth
MAX_FAILED_SHARE = 0.1
MIN_FAILED_TO_RAISE = 5

EQUITY = 'equity'
REGION_US, REGION_INTERNATIONAL, REGION_GLOBAL = 'US', 'International', 'Global'

# Leveraged / inverse by name (the N-CEN flag and FMP's asset class catch most): 2x, 1.5x, Bull /
# Bear, Inverse, Leveraged, "Daily Target" - not "Ultra-Short", "Short Duration" or "Short-Term" bonds.
_LEVERAGED_NAME = re.compile(r'\b\d+(\.\d+)?x\b|\bleveraged\b|\binverse\b|\bbull\b|\bbear\b|\bdaily target\b'
                             r'|\bshort (qqq|spy|s&p|nasdaq|russell|dow|bitcoin|ether)', re.IGNORECASE)
_BUFFER_NAME = re.compile(r'buffer|defined outcome|\bfloor\b|structured (alt )?protection|\bbarrier\b'
                          r'|downside (hedged|protection)|hedged equity|\bprotected\b', re.IGNORECASE)
_OPTION_INCOME_NAME = re.compile(r'covered call|options? income|premium income|buy-?write|\b0dte\b|autocallable'
                                 r'|high income|income target|option strateg|weeklypay|yieldmax|enhanced options',
                                 re.IGNORECASE)
# A holding line that isn't a stock, by its name: cash and currencies, money-market funds,
# derivatives (options - also coded like "NDX_1", "SPX_23" - futures, swaps, forwards), bills and
# notes, coupons ("4.93%") and maturity dates. Stocks FMP lists by name only (JADE's Samsung, SK
# Hynix, Bajaj Finance: no symbol, ISIN or CUSIP) are everything else.
_NON_STOCK_NAME = re.compile(
    r'\bcash\b|money market|\bmmf\b|\bliquidity fund\b|treasury|\bt-?bills?\b|\bbills?\b|\bnotes?\b|\bbonds?\b'
    r'|\bfutures?\b|\boptions?\b|\bcall\b|\bput\b|\bswaps?\b|\bforwards?\b|\bfx\b|\brepo\b|receivable|payable'
    r'|\bmargin\b|collateral|\bdeposits?\b|net other assets|other assets|\bliabilities\b'
    r'|\b(us|u\.s\.|canadian|australian|hong kong|singapore|new zealand|taiwan) dollar\b|\beuro\b|pound sterling'
    r'|\bjapanese yen\b|\byen\b|\bkron[ae]\b|swiss franc|\bwon\b|\brupees?\b|\byuan\b|renminbi|\bpesos?\b'
    r'|%|\b\d{1,2}/\d{1,2}/\d{2,4}\b|^[a-z]{2,5}_\d+$', re.IGNORECASE)


@dataclass
class IndexStock:
    fund: str            # the index fund holding it
    market: str          # 'US' | 'International'
    float_cap: float     # USD


@dataclass
class Reference:
    """Built once per run: the index funds' stocks (stored snapshot), the size breakpoints, and our
    listings for value / growth."""
    index: dict[str, IndexStock]                  # symbol / ISIN / CUSIP -> the index stock
    large_min: dict[str, float]                   # market -> float cap of the 70% breakpoint
    small_below: dict[str, float]                 # market -> float cap of the 90% breakpoint
    listings: index_funds.Listings
    style_of: dict[int, str | None]               # company id -> style_type


@dataclass
class Profile:
    strategy: str | None
    asset_class: str | None
    holdings_lines: int = 0
    stock_holdings: int = 0
    equity_weight: float | None = None
    stock_weight: float | None = None
    top10_weight: float | None = None
    us_weight: float | None = None
    region: str | None = None
    avg_float_cap: float | None = None
    large_weight: float | None = None
    mid_weight: float | None = None
    small_weight: float | None = None
    cap_size: str | None = None
    value_weight: float | None = None
    growth_weight: float | None = None
    style_coverage: float | None = None
    value_growth: str | None = None
    sector_weights: dict | None = None
    top_sector: str | None = None
    top_sector_weight: float | None = None


@dataclass
class ProfileRunStats:
    checked: int = 0
    failed: list[str] = field(default_factory=list)          # "TICKER: error"
    not_on_fmp: int = 0
    by_strategy: Counter = field(default_factory=Counter)
    equity_by_region: Counter = field(default_factory=Counter)
    equity_by_cap: Counter = field(default_factory=Counter)
    etfs_added: int = 0
    etfs_updated: int = 0
    etfs_disabled: int = 0
    holdings_stored: int = 0                                  # funds whose holdings were stored (once)


def _keys(line: dict) -> list[str]:
    return [k for k in (line.get('asset'), line.get('isin'), line.get('securityCusip')) if k]


def build_reference(as_of: date | None = None) -> Reference:
    """The index funds' stocks from their stored snapshot (refreshed first when it's more than a
    week old), the 70% / 90% breakpoints per market, and our listings."""
    index: dict[str, IndexStock] = {}
    snapshot = index_funds.snapshot(as_of)
    for fund, (market, rows) in sorted(snapshot.items(), key=lambda kv: kv[1][0] != index_funds.US):  # US first
        for r in rows:
            if r.get('float_cap'):
                stock = IndexStock(fund=fund, market=market, float_cap=r['float_cap'])
                for key in _keys(r):
                    index.setdefault(key, stock)
    markets = {market for market, _rows in snapshot.values()}
    listings = index_funds.our_listings()
    return Reference(
        index=index,
        large_min={m: index_funds.cutoff(m, LARGE_COVERAGE, as_of) for m in markets},
        small_below={m: index_funds.cutoff(m, MID_COVERAGE, as_of) for m in markets},
        listings=listings,
        style_of={cid: listings.by_id[cid].style_type for cid in listings.members if cid in listings.by_id},
    )


def classify(etf: SecActiveEtf, asset_class: str | None, equity_weight: float | None) -> str:
    """The fund's strategy - see the module docstring. Only 'equity' goes in the test tables."""
    name = etf.fund_name or ''
    ac = (asset_class or '').lower()
    if etf.is_multiple_inverse or 'leveraged' in ac or 'inverse' in ac or _LEVERAGED_NAME.search(name):
        return 'leveraged_inverse'
    if etf.is_fund_of_funds:
        return 'fund_of_funds'
    if _BUFFER_NAME.search(name):
        return 'buffer'
    if _OPTION_INCOME_NAME.search(name):
        return 'option_income'
    for word, strategy in (('fixed income', 'fixed_income'), ('bond', 'fixed_income'), ('multi', 'multi_asset'),
                           ('alternative', 'alternative'), ('commodit', 'commodity'), ('currenc', 'currency')):
        if word in ac:
            return strategy
    if not ac or 'equity' in ac or 'real estate' in ac:
        if equity_weight is not None and equity_weight >= EQUITY_MIN_WEIGHT:
            return EQUITY
        return 'equity_mixed' if ac else 'other'
    return 'other'


def _is_stock_line(line: dict, stock: IndexStock | None) -> bool:
    if stock is not None:
        return True
    name = (line.get('name') or '').strip()
    if not name and not line.get('asset'):
        return False
    return not _NON_STOCK_NAME.search(name)


def profile_fund(etf: SecActiveEtf, info: dict | None, holdings: list[dict], ref: Reference) -> Profile:
    """A fund's strategy and, for an equity fund, its region, cap size, value / growth and
    sectors. Weights are shares of the fund's positive holding weight."""
    asset_class = (info or {}).get('assetClass') or None
    lines = [r for r in holdings if (r.get('weightPercentage') or 0) > 0]
    total = sum(r['weightPercentage'] for r in lines)
    p = Profile(strategy=None, asset_class=asset_class, holdings_lines=len(holdings))
    stock: list[tuple[float, IndexStock]] = []
    if total > 0:
        matched = [(r['weightPercentage'] / total, ref.index.get(next((k for k in _keys(r) if k in ref.index), ''))) for r in lines]
        stock = [(w, s) for w, s in matched if s is not None]
        p.equity_weight = sum(w for (w, s), r in zip(matched, lines) if _is_stock_line(r, s))
        p.stock_weight = sum(w for w, _ in stock)
        p.stock_holdings = len(stock)
        p.top10_weight = sum(sorted((r['weightPercentage'] / total for r in lines), reverse=True)[:10])
    p.strategy = classify(etf, asset_class, p.equity_weight)
    if p.strategy != EQUITY:
        return p

    if p.stock_weight and p.stock_weight >= MIN_COVERAGE:
        sw = p.stock_weight
        p.us_weight = sum(w for w, s in stock if s.market == index_funds.US) / sw
        p.region = REGION_US if p.us_weight >= US_MIN else (REGION_INTERNATIONAL if p.us_weight <= 1 - US_MIN else REGION_GLOBAL)
        p.avg_float_cap = sum(w * s.float_cap for w, s in stock) / sw
        p.large_weight = sum(w for w, s in stock if s.float_cap >= ref.large_min[s.market]) / sw
        p.small_weight = sum(w for w, s in stock if s.float_cap < ref.small_below[s.market]) / sw
        p.mid_weight = max(0.0, 1 - p.large_weight - p.small_weight)
        p.cap_size = ('large' if p.large_weight >= LARGE_MIN else 'small' if p.small_weight >= SMALL_MIN
                      else 'mid' if p.mid_weight >= MID_MIN else 'smid' if p.mid_weight + p.small_weight >= SMID_MIN else 'all')

    if p.cap_size == 'large':  # value / growth only for large-cap funds, from our companies' style
        styled: dict[str, float] = defaultdict(float)
        for r in lines:
            companies = {ref.listings.company_of[tid] for tid in index_funds.matched_listings(r, ref.listings)}
            style = ref.style_of.get(next(iter(companies))) if len(companies) == 1 else None
            if style:
                styled[style] += r['weightPercentage'] / total
        classified = sum(styled.values())
        p.style_coverage = classified / p.equity_weight if p.equity_weight else None
        if classified > 0:
            p.value_weight = styled['value'] / classified
            p.growth_weight = styled['growth'] / classified
            if p.style_coverage and p.style_coverage >= STYLE_MIN_COVERAGE:
                p.value_growth = 'value' if p.value_weight >= STYLE_MIN else 'growth' if p.growth_weight >= STYLE_MIN else 'blend'

    sectors = {s.get('industry'): (s.get('exposure') or 0) / 100 for s in (info or {}).get('sectorsList') or []
               if s.get('industry') and (s.get('exposure') or 0) > 0}
    if sectors:
        p.sector_weights = dict(sorted(sectors.items(), key=lambda kv: -kv[1]))
        top = next(((k, v) for k, v in p.sector_weights.items() if not k.lower().startswith('cash')), None)
        if top:
            p.top_sector, p.top_sector_weight = top
    return p


def _targets(refresh_all: bool, as_of: date) -> list[SecActiveEtf]:
    current = [e for e in sec_active_etf.fetch_current(ncen.filed_since(as_of)) if e.ticker]
    if refresh_all:
        return current
    checked = sec_etf_classification.fetch_all()
    stale = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=PROFILE_REFRESH_DAYS)
    out = []
    for e in current:
        c = checked.get(e.series_id)
        if c is None or c.fmp_error or c.updated_at is None or c.updated_at < stale or (e.updated_at and c.updated_at < e.updated_at):
            out.append(e)
    return out


def _fetch(etf: SecActiveEtf) -> tuple[SecActiveEtf, dict | None, list[dict], Exception | None]:
    try:
        info = api_stocks.get_etf_info(etf.ticker)
        holdings = api_stocks.get_etf_holdings(etf.ticker)
        return etf, info, holdings, None
    except Exception as e:
        return etf, None, [], e


def _holding_date(holdings: list[dict]) -> datetime:
    stamps = [h.get('updatedAt') for h in holdings if h.get('updatedAt')]
    try:
        return datetime.combine(datetime.fromisoformat(max(stamps)).date(), datetime.min.time()) if stamps else \
            datetime.combine(date.today(), datetime.min.time())
    except ValueError:
        return datetime.combine(date.today(), datetime.min.time())


def _save_equity(etf: SecActiveEtf, info: dict, holdings: list[dict], p: Profile, ref: Reference,
                 with_holdings: set[int], stats: ProfileRunStats) -> None:
    provider_id = test_provider.ensure_by_name(info.get('etfCompany') or etf.adviser_name or etf.registrant_name or 'Unknown')
    inception = info.get('inceptionDate')
    etf_id, inserted = test_provider_etf.upsert_profile(TestProviderEtf(
        test_provider_id=provider_id, sec_series_id=etf.series_id,
        region=p.region, name=info.get('name') or etf.fund_name, description=info.get('description'),
        isin=info.get('isin') or None, ticker=etf.ticker, cap_type=p.cap_size, style_type=p.value_growth,
        trading_since=datetime.fromisoformat(inception) if inception else None, website=info.get('website') or None,
        asset_class=p.asset_class, strategy=p.strategy, aum=info.get('assetsUnderManagement'), nav=info.get('nav'),
        nav_currency=info.get('navCurrency'), expense_ratio=info.get('expenseRatio'),
        holdings_lines=p.holdings_lines, stock_holdings=p.stock_holdings, equity_weight=p.equity_weight,
        stock_weight=p.stock_weight, top10_weight=p.top10_weight, us_weight=p.us_weight, avg_float_cap=p.avg_float_cap,
        large_weight=p.large_weight, mid_weight=p.mid_weight, small_weight=p.small_weight,
        value_weight=p.value_weight, growth_weight=p.growth_weight, style_coverage=p.style_coverage,
        sector_weights=p.sector_weights, top_sector=p.top_sector, top_sector_weight=p.top_sector_weight,
    ))
    stats.etfs_added += inserted
    stats.etfs_updated += not inserted
    if etf_id not in with_holdings and holdings:  # the holdings are stored once
        when = _holding_date(holdings)
        test_provider_etf_holding.insert_all([TestProviderEtfHolding(
            test_provider_etf_id=etf_id, holding_date=when, ticker_id=index_funds.matched_ticker_id(h, ref.listings),
            shares=h.get('sharesNumber'), market_value=h.get('marketValue'),
            weight=(h['weightPercentage'] / 100) if h.get('weightPercentage') is not None else None,
            symbol=h.get('asset') or None, name=h.get('name') or None, isin=h.get('isin') or None,
            cusip=h.get('securityCusip') or None,
        ) for h in holdings])
        test_provider_etf.set_last_downloaded(etf_id, datetime.now(timezone.utc).replace(tzinfo=None))
        with_holdings.add(etf_id)
        stats.holdings_stored += 1


def _apply(etf: SecActiveEtf, info: dict | None, holdings: list[dict], error: Exception | None,
           ref: Reference, with_holdings: set[int], stats: ProfileRunStats) -> None:
    stats.checked += 1
    if error is not None:
        stats.failed.append(f"{etf.ticker}: {str(error)[:200]}")
        sec_etf_classification.upsert(SecEtfClassification(series_id=etf.series_id, fmp_error=str(error)[:500]))
        return
    if info is None and not holdings:
        stats.not_on_fmp += 1
        sec_etf_classification.upsert(SecEtfClassification(series_id=etf.series_id, fmp_error='Not on FMP (no info, no holdings)'))
        return
    p = profile_fund(etf, info, holdings, ref)
    stats.by_strategy[p.strategy] += 1
    sec_etf_classification.upsert(SecEtfClassification(
        series_id=etf.series_id, asset_class=p.asset_class, strategy=p.strategy,
        equity_weight=p.equity_weight, stock_weight=p.stock_weight,
        fmp_error=None if holdings else 'No holdings on FMP',
    ))
    if p.strategy == EQUITY:
        stats.equity_by_region[p.region or 'unknown'] += 1
        stats.equity_by_cap[p.cap_size or 'unknown'] += 1
        _save_equity(etf, info or {}, holdings, p, ref, with_holdings, stats)
    else:
        stats.etfs_disabled += test_provider_etf.disable_by_series([etf.series_id], NOT_EQUITY_REASON)


def summary(stats: ProfileRunStats) -> str:
    """A few lines for the cron email."""
    strategies = ", ".join(f"{k} {v}" for k, v in stats.by_strategy.most_common())
    lines = [
        f"SEC ETF profiles (FMP): {stats.checked} fund(s) checked, {len(stats.failed)} failed, {stats.not_on_fmp} not on FMP - {strategies or 'none'}",
        f"Equity funds: by region {dict(stats.equity_by_region)}, by cap size {dict(stats.equity_by_cap)}; "
        f"test provider ETFs {stats.etfs_added} added, {stats.etfs_updated} updated, {stats.etfs_disabled} disabled; "
        f"holdings stored for {stats.holdings_stored}",
    ]
    lines += [f"  failed: {f}" for f in stats.failed[:10]]
    return "\n".join(lines)


def run(refresh_all: bool = False, as_of: date | None = None) -> ProfileRunStats:
    """
    Checks the current SEC active ETFs due for a profile (never checked, a newer filing, or
    PROFILE_REFRESH_DAYS since the last check; every one with refresh_all), saves each one's
    strategy, and places the equity funds, profiled, in the test provider tables - their holdings
    stored the first time. Funds that stopped passing, or left the SEC list, are disabled there.

    Raises - so the cron emails the failure - when the index funds' snapshot can't be read or
    more than MAX_FAILED_SHARE of the funds fail.
    """
    today = as_of or date.today()
    batch_run_id = batch_run.insert(BatchRun(process='sec_etf_profiles', activation='auto'))
    log.record_status(f"Starting SEC ETF profiles batch job ID {batch_run_id}{' (all funds)' if refresh_all else ''}")
    stats = ProfileRunStats()
    try:
        current = {e.series_id for e in sec_active_etf.fetch_current(ncen.filed_since(today))}
        gone = [e.sec_series_id for e in test_provider_etf.fetch_all() if e.sec_series_id and e.sec_series_id not in current]
        stats.etfs_disabled += test_provider_etf.disable_by_series(gone, SEC_GONE_REASON)

        targets = _targets(refresh_all, today)
        log.record_status(f"{len(targets)} SEC active ETF(s) due for a profile.")
        if targets:
            ref = build_reference(today)
            with_holdings = test_provider_etf_holding.fetch_etf_ids_with_holdings()
            pool = ThreadPoolExecutor(max_workers=FETCH_WORKERS)
            try:
                for n, (etf, info, holdings, error) in enumerate(pool.map(_fetch, targets), 1):
                    _apply(etf, info, holdings, error, ref, with_holdings, stats)
                    if n % 250 == 0:
                        print(f"[etf_profile] {n}/{len(targets)} funds checked")
            finally:
                pool.shutdown(wait=True, cancel_futures=True)

        log.record_status(summary(stats))
        if len(stats.failed) > max(MIN_FAILED_TO_RAISE, MAX_FAILED_SHARE * len(targets)):
            raise Exception(f"{len(stats.failed)} of {len(targets)} fund profiles failed - first: {stats.failed[0]}")
        batch_run.update_completed_at(batch_run_id)
        log.record_status("SEC ETF profiles completed.\n")
        return stats

    except Exception as e:
        log.record_error(f"Error in etf_profile: {e}")
        raise
