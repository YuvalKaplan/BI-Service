"""
Finds the actively managed equity funds among the SEC active ETFs (sec_active_etf) from their FMP
data, and places them - with a profile of what they hold - in the provider tables (provider,
provider_etf). The selection rules (modules/sec/etf_selection.py) then decide which of them feed
best ideas; their holdings are downloaded by modules/cron/etf_downloader.py.

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
funds go in provider_etf, profiled by region (US / International / Global), cap size (large / mid /
small / smid / all), value / growth (large-cap funds, from our tickers' style_type), sector
weights (FMP's, else our tickers' sectors), country weights and emerging-market share.

Region, countries and the emerging share go by our company where a line is one (its listings, by
symbol / ISIN / CUSIP) - ADR lines aren't in the index funds, so a fund of them would otherwise
pass for US. Region is the company's region (ticker.region, primary listing - the benchmarks'
rule: Accenture is US), else the market of the index fund holding the stock; country is the
company's domicile (ticker.country), a US-region company counting as US; emerging is held by an
emerging index fund (index_funds.EMERGING_FUNDS - FTSE's view), the company's else the line's.
The index fallback matters for a new fund: its small caps aren't our tickers until its holdings
are downloaded. A provider ETF no longer equity keeps its row with its
new strategy, and the selection makes it inactive. Every checked fund's strategy is kept in
sec_etf_classification, so it's checked again only after PROFILE_REFRESH_DAYS (an active fund:
PROFILE_REFRESH_DAYS_ACTIVE, so the rules read a profile at most a week old) or a newer filing.
"""
import log
import re
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from collections import Counter, defaultdict
from modules.core import api_stocks
from modules.object import batch_run, provider, provider_etf, sec_active_etf, sec_etf_classification
from modules.object.batch_run import BatchRun
from modules.object.provider_etf import ProviderEtf, ACTIVE
from modules.object.sec_active_etf import SecActiveEtf
from modules.object.sec_etf_classification import SecEtfClassification
from modules.sec import etf_selection, ncen
from modules.sec.etf_selection import SelectionStats
from modules.ticker import company, index_funds

PROFILE_REFRESH_DAYS = 28   # a fund's profile is checked again after this long (or after a newer filing)
PROFILE_REFRESH_DAYS_ACTIVE = 6  # ... an active fund's: every Sunday run
FETCH_WORKERS = 4           # funds fetched in parallel, all under api_stocks' 200-calls-a-minute throttle
EQUITY_MIN_WEIGHT = 0.8     # share of the fund in stock lines an equity fund needs
MIN_COVERAGE = 0.5          # share of the fund placed (company or index fund) needed for region, size and emerging
US_MIN = 0.8                # US share of the placed stocks for 'US'; at most 1 - US_MIN for 'International'
LARGE_COVERAGE = 0.70       # a stock is large at or above its market's breakpoint at this coverage...
MID_COVERAGE = 0.90         # ... mid down to this one, small below (Morningstar-style)
LARGE_MIN = 0.7             # cap size: large share for 'large'
SMALL_MIN = 0.5             # ... small share for 'small'
MID_MIN = 0.5               # ... mid share for 'mid'
SMID_MIN = 0.7              # ... mid + small share for 'smid'; otherwise 'all'
STYLE_MIN = 0.6             # value (growth) share of the style-classified weight for 'value' ('growth')
STYLE_MIN_COVERAGE = 0.5    # style-classified share of the stock weight needed for value / growth
SECTOR_MIN_COVERAGE = 0.5   # sector-classified share of the stock weight needed for our own sector weights
COUNTRY_MIN_COVERAGE = 0.5  # country-classified share of the stock weight needed for country weights
MAX_FAILED_SHARE = 0.1
MIN_FAILED_TO_RAISE = 5

EQUITY = etf_selection.EQUITY
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
    listings and companies for region, country, emerging share and value / growth."""
    index: dict[str, IndexStock]                  # symbol / ISIN / CUSIP -> the index stock
    large_min: dict[str, float]                   # market -> float cap of the 70% breakpoint
    small_below: dict[str, float]                 # market -> float cap of the 90% breakpoint
    listings: index_funds.Listings
    style_of: dict[int, str | None]               # company id -> style_type
    fund_of: dict[int, str]                       # company id -> the index fund holding it
    region_of: dict[int, str | None]              # company id -> region (ticker.region)
    country_of: dict[int, str | None]             # company id -> domicile (ticker.country), US for a US-region company


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
    country_weights: dict | None = None
    top_country: str | None = None
    top_country_weight: float | None = None
    emerging_weight: float | None = None


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
    no_longer_equity: int = 0                                 # provider ETFs whose profile isn't equity any more
    sectors_from_holdings: int = 0                            # equity funds FMP has no sector weights for
    no_country_weights: int = 0                               # equity funds too few of whose stocks are our companies
    selection: SelectionStats | None = None


def _keys(line: dict) -> list[str]:
    return [k for k in (line.get('asset'), line.get('isin'), line.get('securityCusip')) if k]


def _line_company(line: dict, listings: index_funds.Listings) -> int | None:
    """The one company of ours a holding line is (its listings by symbol, ISIN or CUSIP), or None."""
    companies = {listings.company_of[tid] for tid in index_funds.matched_listings(line, listings)}
    return next(iter(companies)) if len(companies) == 1 else None


def build_reference(as_of: date | None = None) -> Reference:
    """The index funds' stocks from their stored snapshot (refreshed first when it's more than a
    week old), the 70% / 90% breakpoints per market, and our listings and companies."""
    index: dict[str, IndexStock] = {}
    fund_of: dict[int, str] = {}
    snapshot = index_funds.snapshot(as_of)
    listings = index_funds.our_listings()
    for fund, (market, rows) in sorted(snapshot.items(), key=lambda kv: kv[1][0] != index_funds.US):  # US first
        for r in rows:
            if r.get('float_cap'):
                stock = IndexStock(fund=fund, market=market, float_cap=r['float_cap'])
                for key in _keys(r):
                    index.setdefault(key, stock)
            cid = _line_company(r, listings)
            if cid is not None:
                fund_of.setdefault(cid, fund)
    markets = {market for market, _rows in snapshot.values()}
    region_of = {cid: listings.by_id[cid].region for cid in listings.members if cid in listings.by_id}
    return Reference(
        index=index,
        large_min={m: index_funds.cutoff(m, LARGE_COVERAGE, as_of) for m in markets},
        small_below={m: index_funds.cutoff(m, MID_COVERAGE, as_of) for m in markets},
        listings=listings,
        style_of={cid: listings.by_id[cid].style_type for cid in listings.members if cid in listings.by_id},
        fund_of=fund_of,
        region_of=region_of,
        country_of={cid: 'US' if region_of.get(cid) == REGION_US else company.company_country(members, cid)
                    for cid, members in listings.members.items()},
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


def is_stock_line(line: dict, stock: IndexStock | None) -> bool:
    if stock is not None:
        return True
    name = (line.get('name') or '').strip()
    if not name and not line.get('asset'):
        return False
    return not _NON_STOCK_NAME.search(name)


def profile_fund(etf: SecActiveEtf, info: dict | None, holdings: list[dict], ref: Reference) -> Profile:
    """A fund's strategy and, for an equity fund, its region, cap size, value / growth, sectors,
    countries and emerging share. Weights are shares of the fund's positive holding weight."""
    asset_class = (info or {}).get('assetClass') or None
    lines = [r for r in holdings if (r.get('weightPercentage') or 0) > 0]
    total = sum(r['weightPercentage'] for r in lines)
    p = Profile(strategy=None, asset_class=asset_class, holdings_lines=len(holdings))
    matched: list[tuple[float, IndexStock | None]] = []
    stock: list[tuple[float, IndexStock]] = []
    if total > 0:
        matched = [(r['weightPercentage'] / total, ref.index.get(next((k for k in _keys(r) if k in ref.index), ''))) for r in lines]
        stock = [(w, s) for w, s in matched if s is not None]
        p.equity_weight = sum(w for (w, s), r in zip(matched, lines) if is_stock_line(r, s))
        p.stock_weight = sum(w for w, _ in stock)
        p.stock_holdings = len(stock)
        p.top10_weight = sum(sorted((r['weightPercentage'] / total for r in lines), reverse=True)[:10])
    p.strategy = classify(etf, asset_class, p.equity_weight)
    if p.strategy != EQUITY:
        return p

    # Each line's weight, its company of ours (or None) and its index stock (or None)
    owned = [(w, _line_company(r, ref.listings), s) for (w, s), r in zip(matched, lines)]

    # Region: each stock's company region (the benchmarks' rule), else its index fund's market
    markets, placed = _tally((w, ref.region_of.get(cid) or (s.market if s else None)) for w, cid, s in owned)
    if placed >= MIN_COVERAGE:
        p.us_weight = markets.get(index_funds.US, 0.0) / placed
        p.region = REGION_US if p.us_weight >= US_MIN else (REGION_INTERNATIONAL if p.us_weight <= 1 - US_MIN else REGION_GLOBAL)

    # Emerging: held by an emerging index fund - the company (so an ADR counts), else the line itself
    funds, placed = _tally((w, ref.fund_of.get(cid) or (s.fund if s else None)) for w, cid, s in owned)
    if placed >= MIN_COVERAGE:
        p.emerging_weight = sum(v for f, v in funds.items() if f in index_funds.EMERGING_FUNDS) / placed

    if p.stock_weight and p.stock_weight >= MIN_COVERAGE:
        sw = p.stock_weight
        p.avg_float_cap = sum(w * s.float_cap for w, s in stock) / sw
        p.large_weight = sum(w for w, s in stock if s.float_cap >= ref.large_min[s.market]) / sw
        p.small_weight = sum(w for w, s in stock if s.float_cap < ref.small_below[s.market]) / sw
        p.mid_weight = max(0.0, 1 - p.large_weight - p.small_weight)
        p.cap_size = ('large' if p.large_weight >= LARGE_MIN else 'small' if p.small_weight >= SMALL_MIN
                      else 'mid' if p.mid_weight >= MID_MIN else 'smid' if p.mid_weight + p.small_weight >= SMID_MIN else 'all')

    if p.cap_size == 'large':  # value / growth only for large-cap funds, from our companies' style
        styled, classified = _tally((w, ref.style_of.get(cid)) for w, cid, _s in owned)
        p.style_coverage = classified / p.equity_weight if p.equity_weight else None
        if classified > 0:
            p.value_weight = styled['value'] / classified
            p.growth_weight = styled['growth'] / classified
            if p.style_coverage and p.style_coverage >= STYLE_MIN_COVERAGE:
                p.value_growth = 'value' if p.value_weight >= STYLE_MIN else 'growth' if p.growth_weight >= STYLE_MIN else 'blend'

    sectors = {s.get('industry'): (s.get('exposure') or 0) / 100 for s in (info or {}).get('sectorsList') or []
               if s.get('industry') and (s.get('exposure') or 0) > 0}
    if not sectors:
        sectors = holding_sectors(owned, ref, p.equity_weight)
    if sectors:
        p.sector_weights = dict(sorted(sectors.items(), key=lambda kv: -kv[1]))
        top = next(((k, v) for k, v in p.sector_weights.items() if not k.lower().startswith('cash')), None)
        if top:
            p.top_sector, p.top_sector_weight = top

    countries = holding_countries(owned, ref, p.equity_weight)
    if countries:
        p.country_weights = dict(sorted(countries.items(), key=lambda kv: -kv[1]))
        p.top_country, p.top_country_weight = next(iter(p.country_weights.items()))
    return p


Owned = list[tuple[float, int | None, IndexStock | None]]   # (weight, company id, index stock) per line


def _tally(pairs: Iterable[tuple[float, str | None]]) -> tuple[dict[str, float], float]:
    """({key: weight} over the pairs with a key, their total weight)."""
    out: dict[str, float] = defaultdict(float)
    for w, key in pairs:
        if key:
            out[key] += w
    return out, sum(out.values())


def _company_shares(owned: Owned, of_company: Callable[[int], str | None], equity_weight: float | None,
                    min_coverage: float) -> dict[str, float]:
    """Each line resolving to one of our companies weighs in that company's of_company, as shares
    of the classified weight - {} unless that covers min_coverage of the fund's stock lines."""
    if not equity_weight:
        return {}
    weights, classified = _tally((w, of_company(cid) if cid is not None else None) for w, cid, _s in owned)
    if classified < min_coverage * equity_weight:
        return {}
    return {k: w / classified for k, w in weights.items()}


def holding_sectors(owned: Owned, ref: Reference, equity_weight: float | None) -> dict[str, float]:
    """Sector weights from our tickers when FMP has none (ticker.sector), when they cover
    SECTOR_MIN_COVERAGE of the fund's stock lines."""
    by_id = ref.listings.by_id
    return _company_shares(owned, lambda cid: by_id[cid].sector if cid in by_id else None, equity_weight,
                           SECTOR_MIN_COVERAGE)


def holding_countries(owned: Owned, ref: Reference, equity_weight: float | None) -> dict[str, float]:
    """Country weights from our companies (domicile; a US-region company counts as US), when they
    cover COUNTRY_MIN_COVERAGE of the fund's stock lines."""
    return _company_shares(owned, ref.country_of.get, equity_weight, COUNTRY_MIN_COVERAGE)


def _targets(refresh_all: bool, as_of: date) -> list[SecActiveEtf]:
    current = [e for e in sec_active_etf.fetch_current(ncen.filed_since(as_of)) if e.ticker]
    if refresh_all:
        return current
    checked = sec_etf_classification.fetch_all()
    active = {e.sec_series_id for e in provider_etf.fetch_all() if e.status == ACTIVE and e.sec_series_id}
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    out = []
    for e in current:
        c = checked.get(e.series_id)
        stale = now - timedelta(days=PROFILE_REFRESH_DAYS_ACTIVE if e.series_id in active else PROFILE_REFRESH_DAYS)
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


def _save_equity(etf: SecActiveEtf, info: dict, p: Profile, stats: ProfileRunStats) -> None:
    provider_id = provider.ensure_by_name(info.get('etfCompany') or etf.adviser_name or etf.registrant_name or 'Unknown')
    inception = info.get('inceptionDate')
    _etf_id, inserted = provider_etf.upsert_profile(ProviderEtf(
        provider_id=provider_id, sec_series_id=etf.series_id,
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
        country_weights=p.country_weights, top_country=p.top_country, top_country_weight=p.top_country_weight,
        emerging_weight=p.emerging_weight,
    ))
    stats.etfs_added += inserted
    stats.etfs_updated += not inserted


def _apply(etf: SecActiveEtf, info: dict | None, holdings: list[dict], error: Exception | None,
           ref: Reference, provider_series: set[str], stats: ProfileRunStats) -> None:
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
        stats.sectors_from_holdings += bool(not (info or {}).get('sectorsList') and p.sector_weights)
        stats.no_country_weights += p.country_weights is None
        _save_equity(etf, info or {}, p, stats)
    elif etf.series_id in provider_series:
        provider_etf.set_strategy_by_series(etf.series_id, p.strategy)
        stats.no_longer_equity += 1


def summary(stats: ProfileRunStats) -> str:
    """A few lines for the cron email."""
    strategies = ", ".join(f"{k} {v}" for k, v in stats.by_strategy.most_common())
    lines = [
        f"SEC ETF profiles (FMP): {stats.checked} fund(s) checked, {len(stats.failed)} failed, {stats.not_on_fmp} not on FMP - {strategies or 'none'}",
        f"Equity funds: by region {dict(stats.equity_by_region)}, by cap size {dict(stats.equity_by_cap)}; "
        f"provider ETFs {stats.etfs_added} added, {stats.etfs_updated} updated, {stats.no_longer_equity} no longer equity; "
        f"sector weights from our tickers for {stats.sectors_from_holdings}, no country weights for {stats.no_country_weights}",
    ]
    lines += [f"  failed: {f}" for f in stats.failed[:10]]
    if stats.selection is not None:
        lines.append(etf_selection.summary(stats.selection))
    return "\n".join(lines)


def run(refresh_all: bool = False, as_of: date | None = None) -> ProfileRunStats:
    """
    Checks the current SEC active ETFs due for a profile (never checked, a newer filing, or
    PROFILE_REFRESH_DAYS - an active fund: PROFILE_REFRESH_DAYS_ACTIVE - since the last check;
    every one with refresh_all), saves each one's strategy, and places the equity funds, profiled,
    in provider_etf (new ones pending). Then the selection rules set every provider ETF's status -
    funds no longer equity, or no longer on the SEC list, become inactive there.

    Raises - so the cron emails the failure - when the index funds' snapshot can't be read or
    more than MAX_FAILED_SHARE of the funds fail.
    """
    today = as_of or date.today()
    batch_run_id = batch_run.insert(BatchRun(process='sec_etf_profiles', activation='auto'))
    log.record_status(f"Starting SEC ETF profiles batch job ID {batch_run_id}{' (all funds)' if refresh_all else ''}")
    stats = ProfileRunStats()
    try:
        targets = _targets(refresh_all, today)
        log.record_status(f"{len(targets)} SEC active ETF(s) due for a profile.")
        if targets:
            ref = build_reference(today)
            provider_series = {e.sec_series_id for e in provider_etf.fetch_all() if e.sec_series_id}
            pool = ThreadPoolExecutor(max_workers=FETCH_WORKERS)
            try:
                for n, (etf, info, holdings, error) in enumerate(pool.map(_fetch, targets), 1):
                    _apply(etf, info, holdings, error, ref, provider_series, stats)
                    if n % 250 == 0:
                        print(f"[etf_profile] {n}/{len(targets)} funds checked")
            finally:
                pool.shutdown(wait=True, cancel_futures=True)

        if len(stats.failed) > max(MIN_FAILED_TO_RAISE, MAX_FAILED_SHARE * len(targets)):
            log.record_status(summary(stats))
            raise Exception(f"{len(stats.failed)} of {len(targets)} fund profiles failed - first: {stats.failed[0]}")
        stats.selection = etf_selection.run(today)
        log.record_status(summary(stats))
        batch_run.update_completed_at(batch_run_id)
        log.record_status("SEC ETF profiles completed.\n")
        return stats

    except Exception as e:
        log.record_error(f"Error in etf_profile: {e}")
        raise
