"""
Free float and float factors — safeguards around each company's market cap.

Benchmarks weigh companies by their whole market cap (company_market_cap), the size managers
look at. The free float and the index funds' holdings serve as safeguards around it:

Weekly (refresh):
  1. ticker.free_float — FMP's freeFloat % of every listing (shares-float-all). FMP reports 0 for
     an exchange-traded note or preferred named like its issuer (Algonquin's AQNB, Brookfield
     Renewable's BEPI): company.is_non_equity_line treats such a listing as non-equity. A listing
     an index fund holds is equity whatever FMP says, and keeps no value instead of 0.
  2. ticker.float_factor (company level) — from the index funds' float-adjusted holdings (the
     stored snapshot of universe_etf: VTI, VEA, VWO — modules/ticker/index_funds.py, downloaded
     before this step): a company's market value in the fund, summed over its share classes
     (GOOGL + GOOG), scaled to its float cap, over its company cap. The scale per fund is set so
     the factors sit on FMP's free-float scale (median over held companies of free float x cap /
     market value). A company no index fund holds — MLPs, BDCs, US-sanctioned Chinese companies,
     companies below the index's minimum float (Christian Dior), some US listings of foreign
     companies — takes its primary listing's free float; with neither it has no factor.
     Benchmark membership and the funds' large-cap filter require a factor of at least
     index_funds.MIN_FLOAT_FACTOR; weights don't use it.
  3. Cap check — an index fund can't hold more of a company than the whole company: for one at
     or above its region's large-cap cutoff (index_funds.large_cutoffs), a float cap
     CAP_CHECK_FACTOR x its company cap or more means the company cap is on too few shares
     (Bitmine's history ran on 230M shares against 570M). Flagged in FloatRunStats.cap_checks
     (cron email, scripts/current_float_factors.py); nothing is changed automatically. (A cap on too many shares — Rocket Companies' 3.79B
     against 2.82B — is refresh.verify_share_count's case: FMP's quote and financials against
     its history.)
"""
import log
import math
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from modules.core import api_stocks
from modules.object import ticker, ticker_value
from modules.object.ticker import Ticker
from modules.ticker import company, index_funds, master, pricing, refresh as profile_refresh
from modules.ticker import util as tu
from modules.ticker.resolver import TickerResolver

CAP_CHECK_FACTOR = 1.15  # an index float cap this many times the company cap flags the cap as too low
                         # (fully floated companies sit at 0.97-1.01 — see _value_holdings)
VALUE_LOOKBACK_DAYS = 14  # stored prices and caps a holding is valued from
QUOTE_UNITS = (0.01, 1.0, 100.0)  # our stored price vs the fund's: same unit, or one in minor units


@dataclass
class FloatRunStats:
    listings_with_float: int = 0
    zero_float: list[str] = field(default_factory=list)             # listings FMP reports with no equity float
    index_holdings: dict[str, tuple[int, int]] = field(default_factory=dict)  # fund -> (rows, rows matched to a company)
    scale: dict[str, float] = field(default_factory=dict)            # fund -> float cap per $ held
    by_source: dict[str, int] = field(default_factory=dict)          # fund / 'free_float' / 'unknown' -> companies
    cap_checks: list[str] = field(default_factory=list)              # large caps the index funds hold more of than the whole cap
    cap_fixes: list[str] = field(default_factory=list)               # of those, share counts verified and histories rewritten
    factors_updated: bool = False


def summary(stats: FloatRunStats) -> str:
    """One line for the cron email."""
    funds = ", ".join(f"{f} {m}/{n} holdings matched" for f, (n, m) in stats.index_holdings.items())
    sources = ", ".join(f"{k} {v}" for k, v in stats.by_source.items())
    return (
        f"Free float: {stats.listings_with_float} listings, {len(stats.zero_float)} with no equity float (notes/preferreds); "
        f"index funds: {funds or 'none'}; company float factors by source: {sources or 'not updated'}; "
        f"{len(stats.cap_checks)} large-cap market cap(s) below the index funds' float cap, {len(stats.cap_fixes)} fixed"
    )


def _value_holdings(fund_lines: dict[str, dict[int, list[tuple[int, float, float]]]],
                    by_id: dict[int, Ticker]) -> dict[str, dict[int, float]]:
    """{fund: {company: its holding in USD}}: each line's share count x our price for the listing
    on the date of the company's latest value — the date its cap is from — converted to USD. The
    fund's own marketValue is often out of line with its share count (a stale price), which put
    fully floated companies anywhere from 0.9 to 1.1 of their cap; valued this way they sit at
    0.97-1.01. The quote unit (pence, cents, agorot — not always in ticker.currency) is the one
    closest to the fund's own price per share. A line we have no recent price for keeps the
    fund's marketValue."""
    today = date.today()
    ids = list({tid for lines in fund_lines.values() for cid, ls in lines.items()
                for tid in [cid] + [listing for listing, _shares, _mv in ls]})
    series: dict[int, tuple[dict[date, float], dict[date, float]]] = {}
    for i in range(0, len(ids), 2000):
        series.update(ticker_value.fetch_price_and_cap_series_between(
            ids[i:i + 2000], today - timedelta(days=VALUE_LOOKBACK_DAYS), today))
    rates: dict[str, dict[date, float]] = {}

    def usd_rate(currency: str | None, d: date) -> float | None:
        if not currency or currency == 'USD':
            return 1.0
        if currency not in rates:
            r = pricing.fetch_historic_usd_rates(currency, today - timedelta(days=VALUE_LOOKBACK_DAYS + 7), today)
            rates[currency] = r if isinstance(r, dict) else {}
        return pricing.closest_value_for_date(rates[currency], d, window_days=5) if rates[currency] else None

    out: dict[str, dict[int, float]] = {}
    for fund, lines in fund_lines.items():
        value: dict[int, float] = {}
        for cid, ls in lines.items():
            company_caps = series.get(cid, ({}, {}))[0]
            as_of = max(company_caps) if company_caps else None
            total = 0.0
            for tid, shares, mv in ls:
                t = by_id[tid]
                prices = series.get(tid, ({}, {}))[1]
                price = pricing.closest_value_for_date(prices, as_of, window_days=3) if (as_of and prices) else None
                rate = usd_rate(tu.listing_currency(t.exchange, t.currency), as_of) if price else None
                if price and rate and shares > 0:
                    ours, theirs = price * rate, mv / shares
                    unit = min(QUOTE_UNITS, key=lambda u: abs(math.log(ours / u / theirs)))
                    total += shares * ours / unit
                else:
                    total += mv
            value[cid] = total
        out[fund] = value
    return out


def _fix_flagged(flagged: list[int], by_id: dict[int, Ticker], resolver: TickerResolver,
                 caps: dict[int, float | None]) -> list[str]:
    """Runs the share-count verification (refresh.verify_share_count, the index funds as a
    witness) on each flagged company's own row now, rather than at its next weekly profile
    refresh, so the corrected cap is in place before the universe is built. A company whose count
    is verified gets its history rewritten, then the company caps are refreshed and its float
    factor measured again against the new cap."""
    today = date.today()
    fixes: list[str] = []
    fixed_ids: list[int] = []
    for cid in flagged:
        t = by_id[cid]
        full_symbol = resolver.get_full_symbol(t)
        profile = api_stocks.get_stock_profile(full_symbol)
        if not isinstance(profile, dict):
            continue
        currency = tu.listing_currency(t.exchange, t.currency)
        stored = ticker_value.fetch_price_and_cap_series_between([t.id], pricing.VALUE_HISTORY_START, today).get(t.id)
        verified, why = profile_refresh.verify_share_count(t, full_symbol, profile, stored, currency, today)
        if not why or verified is None:
            continue
        ticker.update_verified_shares(t.id, verified)
        result = pricing.resync_value_history(t.id, full_symbol, currency, verified_shares=verified)
        fixes.append(f"{t.symbol}:{t.exchange} {t.name} — {why}; history rewritten ({result})")
        log.record_notice(f"Free float: {t.symbol}:{t.exchange} {why}, value history rewritten ({result}).")
        fixed_ids.append(cid)
    if fixed_ids:
        master.refresh_company_data()
        new_caps = {t.id: t.company_market_cap for t in ticker.fetch_by_ids(fixed_ids)}
        latest = ticker_value.fetch_latest_market_caps(fixed_ids)
        refactored = []
        for cid in fixed_ids:
            new_cap = new_caps.get(cid) or latest.get(cid)
            if by_id[cid].float_factor and caps.get(cid) and new_cap:
                refactored.append((cid, by_id[cid].float_factor * caps[cid] / new_cap))  # the same float cap, over the new cap
        ticker.update_float_factor_bulk(refactored)
    return fixes


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
    by_sym, by_isin, by_cusip = company.listing_index(valid, full_symbol)

    # 1. Index funds (their stored snapshot - index_funds.refresh, before this step on Wednesdays):
    #    each company's holding lines (the listing, the fund's shares, their value), and which
    #    listings they hold.
    try:
        funds = index_funds.snapshot()
        cutoffs = index_funds.large_cutoffs()
    except Exception as e:
        log.record_notice(f"Free float: index funds unavailable ({e}) — float factors left as they were.")
        funds, cutoffs = {}, {}
    us_funds = [f for f, (market, _rows) in funds.items() if market == index_funds.US]
    held_listings: set[int] = set()
    fund_lines: dict[str, dict[int, list[tuple[int, float, float]]]] = {}
    for fund, (_market, rows) in funds.items():
        if not rows:
            log.record_notice(f"Free float: no holdings for index fund {fund} — float factors left as they were.")
            continue
        lines: dict[int, list[tuple[int, float, float]]] = defaultdict(list)
        matched = 0
        for r in rows:
            by_symbol = by_sym.get(r.get('asset') or '')
            listings = (by_symbol or by_isin.get(r.get('isin') or '')
                        or by_cusip.get(r.get('securityCusip') or '') or set())
            companies = {company_of[tid] for tid in listings}
            if len(companies) != 1 or not r.get('marketValue'):
                continue  # not one of ours, or ambiguous
            matched += 1
            held_listings |= listings
            cid = next(iter(companies))
            listing = next(iter(by_symbol)) if by_symbol else (cid if cid in listings else min(listings))
            inc = index_funds.inclusion(r)  # China A shares are held at a fraction of their float
            lines[cid].append((listing, (r.get('sharesNumber') or 0.0) / inc, r['marketValue'] / inc))
        fund_lines[fund] = lines
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
    if not funds or len(fund_lines) < len(funds):
        return stats  # a fund is missing: keep last week's factors rather than mix sources
    members: dict[int, list[Ticker]] = defaultdict(list)
    for t in valid:
        members[company_of[t.id]].append(t)
    latest = ticker_value.fetch_latest_market_caps(list(members))
    caps = {cid: (by_id[cid].company_market_cap or latest.get(cid)) for cid in members}
    fund_value = _value_holdings(fund_lines, by_id)

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
    checks: list[tuple[float, str]] = []
    flagged: list[int] = []
    for cid in members:
        cap = caps.get(cid)
        held_in = [f for f in funds if cid in fund_value[f] and stats.scale.get(f)]
        if held_in and cap:
            # A company more than one fund holds is measured by the fund of its region.
            in_region = [f for f in held_in if (f in us_funds) == (by_id[cid].region == company.US)]
            fund = (in_region or held_in)[0]
            float_cap = stats.scale[fund] * fund_value[fund][cid]
            factor = float_cap / cap  # above 1: the index holds more than our whole cap (see the cap check)
            sources[fund] += 1
            if index_funds.passes_large(cap, None, cutoffs.get(by_id[cid].region)) and float_cap >= CAP_CHECK_FACTOR * cap:
                t = by_id[cid]
                flagged.append(cid)
                checks.append((float_cap / cap,
                               f"{t.symbol}:{t.exchange} {t.name} — company cap ${cap / 1e9:,.1f}B, {fund} float cap "
                               f"${float_cap / 1e9:,.1f}B ({float_cap / cap:.2f}x): the company cap looks too low (too few shares?)"))
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
    stats.cap_checks = [text for _x, text in sorted(checks, reverse=True)]
    for cid, factor in updates:
        if cid in by_id:
            by_id[cid].float_factor = factor
    stats.cap_fixes = _fix_flagged(flagged, by_id, resolver, caps)
    stats.by_source = dict(sources)
    stats.factors_updated = True
    log.record_status(summary(stats))
    return stats
