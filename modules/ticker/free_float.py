"""
Free float and float factors — how much of each company's market cap is investable.

Managers hold, and are measured against indices that hold, a company in proportion to its free
float, not its whole market cap: an Up-C company's unlisted units (Interactive Brokers, Carvana),
a founder's unlisted class or a fresh IPO's locked-up shares (SpaceX, ~12% floated), a family's
stake (Walmart) aren't bought. Benchmark weights (benchmark_generator.store_holdings, best ideas'
self mode) therefore use company cap x ticker.float_factor, while the company cap itself still
decides size (the $10B floor, the funds' large-cap filter).

Weekly (refresh):
  1. ticker.free_float — FMP's freeFloat % of every listing (shares-float-all). FMP reports 0 for
     an exchange-traded note or preferred named like its issuer (Algonquin's AQNB, Brookfield
     Renewable's BEPI): company.is_non_equity_line treats such a listing as non-equity. A listing
     an index fund holds is equity whatever FMP says, and keeps no value instead of 0.
  2. ticker.float_factor (company level) — from the index funds' float-adjusted holdings
     (INDEX_FUNDS): a company's market value in the fund, summed over its share classes
     (GOOGL + GOOG), scaled to its float cap, over its company cap. The scale per fund is set so
     the factors sit on FMP's free-float scale (median over held companies of free float x cap /
     market value). A company no index fund holds — MLPs, BDCs, US-sanctioned Chinese companies,
     companies below the index's minimum float (Christian Dior), some US listings of foreign
     companies — takes its primary listing's free float; with neither it has no factor and counts
     in full.
"""
import log
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from modules.core import api_stocks
from modules.object import ticker, ticker_value
from modules.object.ticker import Ticker
from modules.ticker import company
from modules.ticker.resolver import TickerResolver

# Vanguard Total Stock Market (CRSP US Total Market) and Total International Stock (FTSE Global
# All Cap ex US): together nearly every listed company, each held at its float-adjusted weight.
INDEX_FUNDS = ('VTI', 'VXUS')
US_FUND = 'VTI'  # a company both hold is measured by the fund of its region


@dataclass
class FloatRunStats:
    listings_with_float: int = 0
    zero_float: list[str] = field(default_factory=list)             # listings FMP reports with no equity float
    index_holdings: dict[str, tuple[int, int]] = field(default_factory=dict)  # fund -> (rows, rows matched to a company)
    scale: dict[str, float] = field(default_factory=dict)            # fund -> float cap per $ held
    by_source: dict[str, int] = field(default_factory=dict)          # fund / 'free_float' / 'unknown' -> companies
    factors_updated: bool = False


def summary(stats: FloatRunStats) -> str:
    """One line for the cron email."""
    funds = ", ".join(f"{f} {m}/{n} holdings matched" for f, (n, m) in stats.index_holdings.items())
    sources = ", ".join(f"{k} {v}" for k, v in stats.by_source.items())
    return (
        f"Free float: {stats.listings_with_float} listings, {len(stats.zero_float)} with no equity float (notes/preferreds); "
        f"index funds: {funds or 'none'}; company float factors by source: {sources or 'not updated'}"
    )


def _index(valid: list[Ticker], full_symbol: dict[int, str]) -> tuple[dict, dict, dict]:
    by_sym, by_isin, by_cusip = defaultdict(set), defaultdict(set), defaultdict(set)
    for t in valid:
        by_sym[full_symbol[t.id]].add(t.id)
        if t.isin:
            by_isin[t.isin].add(t.id)
        if t.cusip:
            by_cusip[t.cusip].add(t.id)
    return by_sym, by_isin, by_cusip


def refresh() -> FloatRunStats:
    """Stores ticker.free_float for every listing and ticker.float_factor for every company.
    An index fund whose holdings can't be fetched leaves last week's factors in place; a failed
    free-float download leaves last week's free floats."""
    stats = FloatRunStats()
    all_t = ticker.fetch_all()
    by_id = {t.id: t for t in all_t}
    valid = [t for t in all_t if not t.invalid]
    resolver = TickerResolver(TickerResolver.POPULATE_TICKER)
    full_symbol = {t.id: resolver.get_full_symbol(t) for t in valid}
    company_of = {t.id: company.company_id(t, by_id) for t in valid}
    by_sym, by_isin, by_cusip = _index(valid, full_symbol)

    # 1. Index funds: market value per company, and which listings they hold.
    held_listings: set[int] = set()
    fund_value: dict[str, dict[int, float]] = {}
    for fund in INDEX_FUNDS:
        rows = api_stocks.get_etf_holdings(fund)
        if not rows:
            log.record_notice(f"Free float: no holdings for index fund {fund} — float factors left as they were.")
            continue
        value: dict[int, float] = defaultdict(float)
        matched = 0
        for r in rows:
            listings = (by_sym.get(r.get('asset') or '') or by_isin.get(r.get('isin') or '')
                        or by_cusip.get(r.get('securityCusip') or '') or set())
            companies = {company_of[tid] for tid in listings}
            if len(companies) != 1 or not r.get('marketValue'):
                continue  # not one of ours, or ambiguous
            matched += 1
            held_listings |= listings
            value[next(iter(companies))] += r['marketValue']
        fund_value[fund] = value
        stats.index_holdings[fund] = (len(rows), matched)

    # 2. Free float per listing.
    try:
        float_rows = api_stocks.get_all_shares_float()
    except Exception as e:
        log.record_notice(f"Free float: download failed ({e}) — free floats left as they were.")
        float_rows = None
    if float_rows is not None:
        free: dict[int, float | None] = {}
        for r in float_rows:
            for tid in by_sym.get(r.get('symbol') or '', ()):
                free[tid] = r.get('freeFloat')
        # FMP's 0 marks a note or preferred — which has an ISIN of its own. A 0 on a listing an
        # index fund holds, or sharing its ISIN with one that is held or has a float (the same
        # shares on another venue: Kingspan on LSE, Ahold Delhaize in Milan, an order-book line),
        # is a data gap instead.
        equity_isins = {by_id[tid].isin for tid in held_listings if by_id[tid].isin}
        equity_isins |= {by_id[tid].isin for tid, ff in free.items() if ff and by_id[tid].isin}
        for tid, ff in free.items():
            if ff == 0 and (tid in held_listings or by_id[tid].isin in equity_isins):
                free[tid] = None
        ticker.update_free_float_bulk([(t.id, free.get(t.id)) for t in valid])
        for t in valid:
            t.free_float = free.get(t.id)
        stats.listings_with_float = sum(1 for v in free.values() if v is not None)
        stats.zero_float = sorted(f"{by_id[tid].symbol}:{by_id[tid].exchange} ({by_id[tid].name})"
                                  for tid, v in free.items() if v == 0)

    # 3. Float factor per company.
    if len(fund_value) < len(INDEX_FUNDS):
        return stats  # a fund is missing: keep last week's factors rather than mix sources
    members: dict[int, list[Ticker]] = defaultdict(list)
    for t in valid:
        members[company_of[t.id]].append(t)
    latest = ticker_value.fetch_latest_market_caps(list(members))
    caps = {cid: (by_id[cid].company_market_cap or latest.get(cid)) for cid in members}

    def primary_free_float(cid: int) -> float | None:
        own = by_id[cid].free_float if cid in by_id else None
        if own is not None:
            return own
        return next((t.free_float for t in members[cid] if t.free_float is not None), None)

    for fund, value in fund_value.items():
        ratios = [primary_free_float(cid) / 100 * caps[cid] / mv for cid, mv in value.items()
                  if mv > 0 and caps.get(cid) and primary_free_float(cid)]
        stats.scale[fund] = statistics.median(ratios) if ratios else 0.0

    updates: list[tuple[int, float | None]] = []
    sources: dict[str, int] = defaultdict(int)
    for cid in members:
        cap = caps.get(cid)
        held_in = [f for f in INDEX_FUNDS if cid in fund_value[f] and stats.scale.get(f)]
        if held_in and cap:
            if len(held_in) > 1:
                region_fund = US_FUND if by_id[cid].region == company.US else next(f for f in held_in if f != US_FUND)
                fund = region_fund if region_fund in held_in else held_in[0]
            else:
                fund = held_in[0]
            factor = min(1.0, stats.scale[fund] * fund_value[fund][cid] / cap)
            sources[fund] += 1
        elif (ff := primary_free_float(cid)) is not None:
            factor = min(1.0, max(0.0, ff / 100))
            sources['free_float'] += 1
        else:
            factor = None
            sources['unknown'] += 1
        updates.append((cid, factor))
    # A listing that stopped being its company's master (a realignment) drops its old factor.
    updates += [(t.id, None) for t in valid if company_of[t.id] != t.id and t.float_factor is not None]
    ticker.update_float_factor_bulk(updates)
    stats.by_source = dict(sources)
    stats.factors_updated = True
    log.record_status(summary(stats))
    return stats
