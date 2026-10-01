"""
The market as the index funds see it, and where its size breakpoints fall.

The index funds (universe_etf: VTI, CRSP US Total Market, for the US; VEA, FTSE Developed All Cap
ex US, and VWO, FTSE Emerging All Cap, together International) hold nearly every listed company
at its float-adjusted weight. Weekly (refresh, Wednesday before the free float):

  1. Each fund's holdings are downloaded (FMP etf/holdings) and stored as a snapshot
     (universe_etf_holding), so every decision based on them can be traced back.
  2. A fund's lines are grouped into companies - by our company (share classes and listings of
     one company), else by the CUSIP's issuer part - and valued as float caps: the company's value
     in the fund times the fund's scale (float cap per $ held: median over our companies of free
     float x company cap / value). Stored on each line (float_cap).
  3. The market's breakpoints: the float cap at which its largest companies make up each coverage
     (BREAKPOINT_COVERAGES) of its total float cap (market_breakpoint).

The breakpoints set size relative to the market rather than at fixed dollar lines:
  - each benchmark's large-cap cutoff is its market's breakpoint at benchmark.market_coverage
    (US 0.93 - the Russell 1000's share of the market; International 0.80), and a company is in
    when its whole company cap reaches it and at least MIN_FLOAT_FACTOR of it floats (passes_large -
    the S&P / Russell way: membership on the whole cap, a minimum float);
  - the funds' large / mid_small filter and the universe screener use the same cutoffs
    (large_cutoffs);
  - the SEC ETF profiles size their stocks by the 0.70 / 0.90 breakpoints (Morningstar-style),
    and count the stocks the emerging fund holds (EMERGING_FUNDS - FTSE's view: Korea is
    developed) as a fund's emerging-market share.
"""
import log
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from modules.core import api_stocks
from modules.object import benchmark, market_breakpoint, ticker, universe_etf, universe_etf_holding
from modules.object.market_breakpoint import MarketBreakpoint
from modules.object.ticker import Ticker
from modules.object.universe_etf_holding import UniverseEtfHolding
from modules.ticker import company
from modules.ticker.resolver import TickerResolver

US = company.US
INTERNATIONAL = company.INTERNATIONAL
MIN_FLOAT_FACTOR = 0.10                                                    # S&P's minimum float (IWF)
BREAKPOINT_COVERAGES = [round(0.50 + i / 100, 2) for i in range(50)]      # 0.50 - 0.99, every week
SCREEN_MARGIN = 0.8        # the universe screener fetches from this share of the lowest benchmark cutoff
SNAPSHOT_MAX_AGE_DAYS = 7  # an older snapshot is refreshed before it's used
FULL_UNIVERSE_STYLES = ('blend', 'core')
EMERGING_FUNDS = ('VWO',)  # the index funds whose stocks are emerging markets (FTSE Emerging: Korea isn't)
# FTSE includes China A shares (Shanghai / Shenzhen listings - VWO's .SS / .SZ lines) at 25% of
# their investable float, so their holding is scaled up by it before it's valued: 2,077 of VWO's
# 5,035 lines are A shares, 6% of its weight, and left as held they pulled VWO's scale ~20% up
# (flagging South African and Brazilian caps as too low) and put the A-share companies at a
# quarter of their float.
CHINA_A_INCLUSION = 0.25
_PARTIAL_INCLUSION_SUFFIXES = ('.SS', '.SZ')


def inclusion(line: dict) -> float:
    """The share of its float the index holds a line's company at, where that isn't all of it."""
    return CHINA_A_INCLUSION if (line.get('asset') or '').upper().endswith(_PARTIAL_INCLUSION_SUFFIXES) else 1.0


@dataclass
class Listings:
    """Our valid listings, indexed to match fund holding lines (symbol, then ISIN, then CUSIP)."""
    by_id: dict[int, Ticker]
    by_sym: dict
    by_isin: dict
    by_cusip: dict
    company_of: dict[int, int]            # listing id -> company id
    members: dict[int, list[Ticker]]      # company id -> its listings


@dataclass
class RefreshStats:
    as_of: date | None = None
    lines: dict[str, int] = field(default_factory=dict)          # fund -> lines stored
    scale: dict[str, float] = field(default_factory=dict)        # fund -> float cap per $ held
    breakpoints: dict[tuple[str, float], tuple[float, int]] = field(default_factory=dict)  # (market, coverage) -> (float cap, companies)


def our_listings() -> Listings:
    all_t = ticker.fetch_all()
    by_id = {t.id: t for t in all_t}
    valid = [t for t in all_t if not t.invalid]
    resolver = TickerResolver(TickerResolver.POPULATE_TICKER)
    by_sym, by_isin, by_cusip = company.listing_index(valid, {t.id: resolver.get_full_symbol(t) for t in valid})
    company_of = {t.id: company.company_id(t, by_id) for t in valid}
    members: dict[int, list[Ticker]] = defaultdict(list)
    for t in valid:
        members[company_of[t.id]].append(t)
    return Listings(by_id=by_id, by_sym=by_sym, by_isin=by_isin, by_cusip=by_cusip, company_of=company_of, members=members)


def matched_listings(line: dict, ls: Listings) -> set[int]:
    """Our listings an FMP holding line (asset, isin, securityCusip) is."""
    return (ls.by_sym.get(line.get('asset') or '') or ls.by_isin.get(line.get('isin') or '')
            or ls.by_cusip.get(line.get('securityCusip') or '') or set())


def matched_ticker_id(line: dict, ls: Listings) -> int | None:
    """The ticker a holding line is: the listing its symbol is, else its company (by ISIN / CUSIP)."""
    by_symbol = ls.by_sym.get(line.get('asset') or '')
    if by_symbol:
        return min(by_symbol)
    companies = {ls.company_of[tid] for tid in matched_listings(line, ls)}
    return next(iter(companies)) if len(companies) == 1 else None


def _free_float(cid: int, ls: Listings) -> float | None:
    own = ls.by_id[cid].free_float if cid in ls.by_id else None
    return own if own is not None else next((t.free_float for t in ls.members[cid] if t.free_float is not None), None)


def company_float_caps(rows: list[dict], ls: Listings) -> tuple[dict[object, float], list[object], float]:
    """A fund's companies valued as float caps: ({company key: float cap}, the company key of each
    line, the fund's scale). A company's lines (share classes, listings) are one: grouped by our
    company, else by the CUSIP's issuer part (its first six characters), else the line alone."""
    keys: list[object] = []
    value: dict[object, float] = defaultdict(float)
    for i, r in enumerate(rows):
        companies = {ls.company_of[tid] for tid in matched_listings(r, ls)}
        cusip = r.get('securityCusip') or ''
        key = ('c', next(iter(companies))) if len(companies) == 1 else (('i', cusip[:6]) if len(cusip) >= 6 else ('l', i))
        keys.append(key)
        value[key] += (r.get('marketValue') or 0.0) / inclusion(r)
    ratios = [_free_float(k[1], ls) / 100 * ls.by_id[k[1]].company_market_cap / v for k, v in value.items()
              if k[0] == 'c' and v > 0 and k[1] in ls.by_id and ls.by_id[k[1]].company_market_cap and _free_float(k[1], ls)]
    if not ratios:
        raise Exception("No company to scale the fund's holdings by (no company cap and free float among its holdings)")
    scale = statistics.median(ratios)
    return {k: v * scale for k, v in value.items()}, keys, scale


def breakpoints(float_caps: list[float], coverages: list[float]) -> dict[float, tuple[float, int]]:
    """{coverage: (float cap, companies)}: the float cap of the company at which the largest ones
    make up `coverage` of the total, and how many there are down to it."""
    caps = sorted((c for c in float_caps if c > 0), reverse=True)
    total = sum(caps)
    out: dict[float, tuple[float, int]] = {}
    cumulative = 0.0
    pending = sorted(coverages)
    for n, cap in enumerate(caps, 1):
        cumulative += cap
        while pending and cumulative >= pending[0] * total:
            out[pending.pop(0)] = (cap, n)
        if not pending:
            break
    return out


def holdings_date(rows: list[dict]) -> date | None:
    """The date of a fund's FMP holdings (etf/holdings): its lines' latest updatedAt - None when
    they have none."""
    stamps = [r.get('updatedAt') for r in rows if r.get('updatedAt')]
    try:
        return datetime.fromisoformat(max(stamps)).date() if stamps else None
    except ValueError:
        return None


def summary(stats: RefreshStats) -> str:
    """One line for the cron email."""
    funds = ", ".join(f"{f} {n:,} lines (scale {stats.scale.get(f, 0):.1f})" for f, n in stats.lines.items())
    shown = [(m, c) for m, c in stats.breakpoints if c in (0.70, 0.80, 0.90, 0.93)]
    points = "; ".join(f"{m} {c:.0%} ${stats.breakpoints[(m, c)][0] / 1e9:,.1f}B ({stats.breakpoints[(m, c)][1]:,} companies)"
                       for m, c in sorted(shown))
    return f"Index funds {stats.as_of}: {funds}. Breakpoints: {points}"


def refresh(as_of: date | None = None) -> RefreshStats:
    """Downloads every enabled index fund's holdings, stores them (with each line's company float
    cap) and the market breakpoints they give. Raises when a fund's holdings can't be fetched or
    valued - the stored snapshot and breakpoints then stay as they were."""
    funds = universe_etf.fetch_enabled()
    if not funds:
        raise Exception("No enabled index funds in universe_etf.")
    ls = our_listings()
    rows = {}
    for f in funds:
        rows[f.symbol] = api_stocks.get_etf_holdings(f.symbol)
        if not rows[f.symbol]:
            raise Exception(f"No holdings for index fund {f.symbol} - snapshot and breakpoints left as they were.")

    stats = RefreshStats()
    market_caps: dict[str, dict[object, float]] = defaultdict(dict)   # market -> company key -> float cap
    snapshots = []
    for f in sorted(funds, key=lambda f: f.market != US):              # US first: a company both hold is US
        caps, keys, scale = company_float_caps(rows[f.symbol], ls)
        stats.scale[f.symbol] = scale
        for key, cap in caps.items():
            # One of our companies counts once, in the first market holding it; issuer / line keys
            # are per fund, so only deduplicated within a market.
            if key[0] == 'c' and any(key in caps_of for caps_of in market_caps.values()):
                continue
            market_caps[f.market].setdefault(key, cap)
        when = datetime.combine(holdings_date(rows[f.symbol]) or date.today(), datetime.min.time())
        snapshots.append((f, when, [UniverseEtfHolding(
            universe_etf_id=f.id, holding_date=when, ticker_id=matched_ticker_id(r, ls),
            shares=r.get('sharesNumber'), market_value=r.get('marketValue'),
            weight=(r['weightPercentage'] / 100) if r.get('weightPercentage') is not None else None,
            symbol=r.get('asset') or None, name=r.get('name') or None, isin=r.get('isin') or None,
            cusip=r.get('securityCusip') or None, float_cap=caps[keys[i]],
        ) for i, r in enumerate(rows[f.symbol])]))

    stats.as_of = max(when for _f, when, _items in snapshots).date()
    points = []
    for market, caps in market_caps.items():
        for coverage, (cap, n) in breakpoints(list(caps.values()), BREAKPOINT_COVERAGES).items():
            stats.breakpoints[(market, coverage)] = (cap, n)
            points.append(MarketBreakpoint(as_of_date=stats.as_of, market=market, coverage=coverage, float_cap=cap, companies=n))

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    for f, when, items in snapshots:
        universe_etf_holding.replace_snapshot(f.id, when, items)
        universe_etf.set_last_downloaded(f.id, now)
        stats.lines[f.symbol] = len(items)
    market_breakpoint.replace_for_date(stats.as_of, points)
    log.record_status(summary(stats))
    return stats


def snapshot(as_of: date | None = None) -> dict[str, tuple[str, list[dict]]]:
    """{fund: (market, lines)}: each enabled index fund's stored holdings from the latest date on or
    before as_of, as FMP-shaped lines (asset, isin, securityCusip, sharesNumber, marketValue,
    weightPercentage) with their float_cap and ticker_id. A fund with no snapshot, or one older
    than SNAPSHOT_MAX_AGE_DAYS, is downloaded first (refresh)."""
    as_of = as_of or date.today()

    def read() -> tuple[dict, bool]:
        out, stale = {}, False
        for f in universe_etf.fetch_enabled():
            items = universe_etf_holding.fetch_latest(f.id, as_of)
            if not items or (as_of - items[0].holding_date.date()).days > SNAPSHOT_MAX_AGE_DAYS:
                stale = True
            out[f.symbol] = (f.market, [{
                'asset': h.symbol or '', 'isin': h.isin or '', 'securityCusip': h.cusip or '', 'name': h.name,
                'sharesNumber': h.shares, 'marketValue': h.market_value,
                'weightPercentage': h.weight * 100 if h.weight is not None else None,
                'float_cap': h.float_cap, 'ticker_id': h.ticker_id,
            } for h in items])
        return out, stale

    out, stale = read()
    if stale and as_of >= date.today() - timedelta(days=1):
        refresh()
        out, _ = read()
    return out


def cutoff(market: str, coverage: float, as_of: date | None = None) -> float:
    """The market's breakpoint at this coverage from the latest snapshot on or before as_of (the
    earliest one for a date before breakpoints were kept). With none stored yet, the index funds
    are downloaded first."""
    as_of = as_of or date.today()
    found = market_breakpoint.fetch_float_cap(market, coverage, as_of)
    if found is None:
        refresh()
        found = market_breakpoint.fetch_float_cap(market, coverage, as_of)
    if found is None:
        raise Exception(f"No {market} market breakpoint at {coverage:.0%} - the index funds couldn't be read.")
    return found[1]


def full_universe_coverage() -> dict[str, float]:
    """{region: coverage} of each region's full-universe benchmark (the enabled large-cap blend /
    core row) - the large-cap line the funds' filter and the screener share with it."""
    out: dict[str, float] = {}
    for b in benchmark.fetch_all():
        if b.cap_type == 'large' and b.style_type in FULL_UNIVERSE_STYLES:
            out.setdefault(b.region, b.market_coverage)
    if not out:
        raise Exception("No enabled large-cap blend benchmark to take the large-cap coverage from.")
    return out


def large_cutoffs(as_of: date | None = None) -> dict[str, float]:
    """{region: large-cap cutoff} as of the date: each region's full-universe benchmark's."""
    return {region: cutoff(region, coverage, as_of) for region, coverage in full_universe_coverage().items()}


def passes_large(whole_cap: float | None, float_factor: float | None, cut: float | None) -> bool:
    """The S&P / Russell way: the whole company cap reaches the cutoff, and at least
    MIN_FLOAT_FACTOR of it floats (no factor known: not held against it)."""
    if not whole_cap or cut is None:
        return False
    return whole_cap >= cut and (float_factor is None or float_factor >= MIN_FLOAT_FACTOR)
