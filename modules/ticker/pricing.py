import log
import statistics
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from zoneinfo import ZoneInfo

from modules.core import api_stocks
from modules.object import ticker
from modules.object.ticker_value import TickerValue, fetch_values_for_ticker, fetch_latest_value_date, upsert as _upsert_tv, replace_range as _replace_range
from modules.ticker import util as tu

HISTORY_LOOKBACK_DAYS = 21          # calendar days - comfortably covers 10+ trading days
                                    # even across a holiday-heavy stretch
PRICE_TOLERANCE       = 0.005      # 0.5% relative diff
MARKET_CAP_TOLERANCE  = 0.01       # 1.0% relative diff (looser: also depends on shares outstanding)
GRACE_PERIOD_DAYS     = 5          # consecutive calendar days a ticker may go unrecorded while
                                    # mismatching before it gets flagged invalid
REVISION_TOLERANCE    = 0.05       # every mismatch within this: FMP revised its history - its values replace ours
BASIS_SPREAD          = 1.06       # only prices or only caps off, this steadily, on every mismatching day: our
BASIS_MIN_POINTS      = 5          # history is on another basis (currency, split) - rewritten; at least this many
                                   # days (an exchange rate moves a few % over the days compared: NZD 3.1% in Sep 2026)
REVISION, BASIS = 'revision', 'basis'
MISMATCH_REASON = "Price/market cap mismatch vs FMP history"   # how ticker.invalid starts for a mismatch
VALUE_DATE_CUT_OFF_HOUR = 17       # New York time: before it, the latest completed trading day is the previous one


def latest_value_date(now: datetime | None = None) -> date:
    """The date today's values are stored for — the latest completed trading day: today in New
    York from 17:00 ET, else the previous day, stepped back over a weekend. Shared by the
    holdings resolution and the screener, so a listing both see is validated once."""
    now_et = (now or datetime.now(ZoneInfo("America/New_York"))).astimezone(ZoneInfo("America/New_York"))
    d = (now_et if now_et.hour >= VALUE_DATE_CUT_OFF_HOUR else now_et - timedelta(days=1)).date()
    while d.weekday() >= 5:  # Saturday=5, Sunday=6
        d -= timedelta(days=1)
    return d

_fx_history_cache: dict[tuple[str, date, date], dict[date, float]] = {}


def fetch_historic_usd_rates(currency: str | None, start: date, end: date) -> dict[date, float] | str:
    """
    Cached {date: rate_to_usd} for every day FMP has a quote for `currency`->USD over
    [start, end]. Using the actual per-date historical rate (not a single "spot" rate applied
    blanket across a whole range) matters here — FX rates move meaningfully over time (e.g.
    KRWUSD moved ~6% between 2026-01-01 and 2026-09-23), so a value from January needs
    January's rate, not today's.

    Returns {} (not an error) when currency is None/USD, since no conversion is needed then —
    callers should treat an empty dict as "use the raw value as-is". Returns an error string if
    the currency is unrecognized or the fetch fails; callers should treat that the same as any
    other "data unavailable" case (skip/withhold) rather than store an unconverted, wrong-unit
    value. Cached per (currency, start, end) since many tickers on the same exchange typically
    share both currency and requested date range within a single run.
    """
    if not currency or currency == 'USD':
        return {}
    key = (currency, start, end)
    if key in _fx_history_cache:
        return _fx_history_cache[key]
    raw = api_stocks.get_historic_fx_rates(currency, 'USD', start, end)
    if isinstance(raw, str):
        log.record_notice(f"Historic FX rates unavailable for {currency}->USD: {raw}")
        return raw
    rates: dict[date, float] = {}
    for row in raw:
        try:
            rates[date.fromisoformat(row["date"])] = float(row["price"])
        except (KeyError, ValueError, TypeError):
            continue
    _fx_history_cache[key] = rates
    return rates


def closest_value_for_date(series: dict[date, float], target: date, window_days: int = 5) -> float | None:
    """Nearest available value to `target` within `series`, searching outward day-by-day up to
    window_days in either direction — a data series doesn't always share the same trading-day
    calendar as whatever it's being matched against (e.g. FX markets trade on different
    holidays than a given stock exchange)."""
    for offset in range(window_days + 1):
        for d in (target - timedelta(days=offset), target + timedelta(days=offset)):
            if d in series:
                return series[d]
    return None


GLITCH_FACTOR = 3.0            # a day-over-day market-cap move this large, not matched by the price, is an FMP glitch
EXTREME_GLITCH_FACTOR = 20.0   # a move this large is a glitch even when FMP scaled the price with it (pence vs pounds)
MAX_EDGE_GLITCH_POINTS = 65    # without prices, a jump at a series' start/end lasting longer than this (~3 months) is a real level change
OUTLIER_CONTEXT_DAYS = 365     # history fetched around a new value so market_cap_outliers can tell what's normal
REPAIR_REFERENCE_POINTS = 5    # good values nearest a glitch whose implied share count repairs it
REFERENCE_TRIGGER_FACTOR = 2.0 # a cap move this large the price didn't make is worth checking against the profile
REFERENCE_FACTOR = 2.5         # an implied share count this far off the profile's is a glitch (real share counts
                               # don't move 2.5x in a year; Alphabet's all-classes/class-A ratio is ~2.1)
SHARE_COUNT_TOLERANCE = 1.15   # an implied share count this far off a listing's verified one (ticker.verified_shares)
                               # is FMP's history on a wrong count (Rocket Companies: 3.79B against 2.82B)
LATEST_RUN_TOLERANCE = 1.05    # implied share counts within this of the latest one are the same run of FMP's history


def _ratio(a: float, b: float) -> float:
    return max(a, b) / min(a, b)


def _is_glitch_jump(cap_a: float, cap_b: float, shares_a: float | None, shares_b: float | None) -> bool:
    """A market-cap jump that isn't the market: GLITCH_FACTOR-fold, and — when prices are known —
    the implied share count (cap / price) jumps with it (a crash moves price and cap together:
    EyePoint -67% on 2026-08-17 is real), unless it's EXTREME_GLITCH_FACTOR-fold."""
    cap_ratio = _ratio(cap_a, cap_b)
    if cap_ratio < GLITCH_FACTOR:
        return False
    if cap_ratio >= EXTREME_GLITCH_FACTOR or not shares_a or not shares_b:
        return True
    return _ratio(shares_a, shares_b) >= GLITCH_FACTOR


def market_cap_outliers(
    series: dict[date, float], prices: dict[date, float] | None = None, reference_shares: float | None = None,
) -> set[date]:
    """
    Dates whose market cap is an FMP data glitch. FMP's history carries a wrong unit or share
    count now and then, for a day or for months — Compass at 1/100 (pence vs pounds) from
    2026-06-24 to 07-02, Shoprite at 1/100 for ten weeks, a NYSE line x266 every weekend, and a
    late-July share-count change that put Hyundai Glovis at x7.7 and Prudential's Hong Kong line
    at x10 from then on while fixing Centrica's, SPIE's and Hanwha Ocean's older history — and one
    such value is enough to push a company out of the $10B benchmark or swell its weight.

    With `reference_shares` (FMP's current share count for the listing, reference_shares()) and
    prices, a value is a glitch when its implied share count (cap / price) is REFERENCE_FACTOR-fold
    off the reference — which side of a jump is wrong can't be told from the series alone
    (Glovis's recent values are wrong, Centrica's older ones — and the Japanese banks' OTC lines'
    whole history since April, off by the yen rate, against three right months). The profile
    can be wrong too (SABESP's implied 3.5B shares against its 685M), and then it matches a
    glitch — an eight-day streak in August that came back — so the reference is ignored when
    every value it agrees with is a spike or streak that comes back (the structural rules'
    strongest evidence). A reference that agrees with none of the series is trusted only for a
    new line (fewer than MAX_EDGE_GLITCH_POINTS priced values), where FMP's history is the weak
    side (Uniper's new XETRA line at 8.7B shares, before its 1:20 consolidation, against 416M);
    against an established history it's more likely the profile that's off. Then, and for values
    without a price or glitches FMP scaled the price along with (pence vs pounds), the
    structural rules (_structural_outliers) decide.
    """
    if reference_shares and prices:
        priced = [d for d, v in series.items() if v and v > 0 and prices.get(d)]
        off = {d for d in priced if _ratio(series[d] / prices[d], reference_shares) >= REFERENCE_FACTOR}
        agree = {d for d in priced if d not in off}
        trusted = (not agree and len(priced) < MAX_EDGE_GLITCH_POINTS) or (
            agree and not agree <= _structural_outliers(series, prices, middle_only=True))
        if off and trusted:
            return off | _structural_outliers({d: v for d, v in series.items() if d not in off}, prices)
    return _structural_outliers(series, prices)


def _needs_reference(series: dict[date, float], prices: dict[date, float]) -> bool:
    """Whether a series the structural rules found clean is still worth one profile call: too
    short to judge from itself (a new listing — FMP's early share counts are often wrong), or
    with a cap move of REFERENCE_TRIGGER_FACTOR the price didn't make (DuPont's share count went
    410M -> 137M on a flat price: exactly 3x, just under GLITCH_FACTOR)."""
    dates = sorted(d for d, v in series.items() if v and v > 0)
    if len(dates) < MAX_EDGE_GLITCH_POINTS:
        return True
    for a, b in zip(dates, dates[1:]):
        if (prices.get(a) and prices.get(b) and _ratio(series[a], series[b]) >= REFERENCE_TRIGGER_FACTOR
                and _ratio(series[a] / prices[a], series[b] / prices[b]) >= REFERENCE_TRIGGER_FACTOR):
            return True
    return False


def _structural_outliers(
    series: dict[date, float], prices: dict[date, float] | None = None, middle_only: bool = False,
) -> set[date]:
    """
    Glitches judged from the series itself. A company's market cap doesn't move
    GLITCH_FACTOR-fold from one day to the next without its price doing the same (a split
    changes neither), so the series is cut into runs at every such jump (_is_glitch_jump;
    `prices` enables the price check) and:
      - a run whose neighbours on both sides are both far higher, or both far lower — a spike or
        a streak that comes back — is a glitch, however long;
      - otherwise a run at the start or end of the series is a glitch when it's shorter than
        its neighbour and either its jump wasn't matched by the price (the implied share count
        jumped) or, prices unknown, it's at most MAX_EDGE_GLITCH_POINTS long (a longer one is
        taken as a real change of level); a run between a lower and a higher one (a staircase)
        is left alone.
    Glitches in the middle are removed first, shortest first (a short normal stretch at the end
    must not be mistaken for the glitch), then the series is cut again. Non-positive values
    are always outliers. middle_only stops before the start/end rule — only the spikes and
    streaks that come back.
    """
    prices = prices or {}
    shares = {d: v / prices[d] for d, v in series.items() if v and v > 0 and prices.get(d)}
    out = {d for d, v in series.items() if not v or v <= 0}
    kept = sorted(d for d in series if d not in out)
    while len(kept) > 1:
        runs: list[list[date]] = [[kept[0]]]
        for prev, d in zip(kept, kept[1:]):
            if _is_glitch_jump(series[prev], series[d], shares.get(prev), shares.get(d)):
                runs.append([d])
            else:
                runs[-1].append(d)
        if len(runs) < 2:
            break
        levels = [statistics.median(series[d] for d in run) for run in runs]
        share_levels = [
            statistics.median(shares[d] for d in run) if all(d in shares for d in run) else None
            for run in runs
        ]

        def far(i: int, j: int) -> bool:
            return _is_glitch_jump(levels[i], levels[j], share_levels[i], share_levels[j])

        def price_unmatched(i: int, j: int) -> bool:
            return bool(share_levels[i] and share_levels[j]) and _ratio(share_levels[i], share_levels[j]) >= GLITCH_FACTOR

        middle: list[int] = []
        edges: list[int] = []
        for i, run in enumerate(runs):
            neighbours = [j for j in (i - 1, i + 1) if 0 <= j < len(runs)]
            if not all(far(i, j) for j in neighbours):
                continue
            if len(neighbours) == 2:
                if (levels[i - 1] > levels[i]) == (levels[i + 1] > levels[i]):
                    middle.append(i)
            elif len(run) < len(runs[neighbours[0]]) and (
                    len(run) <= MAX_EDGE_GLITCH_POINTS or price_unmatched(i, neighbours[0])):
                edges.append(i)
        candidates = middle or ([] if middle_only else edges)
        if not candidates:
            break
        glitch = set(runs[min(candidates, key=lambda i: (len(runs[i]), i))])
        out |= glitch
        kept = [d for d in kept if d not in glitch]
    return out


def reference_shares(symbol: str) -> float | None:
    """FMP's current share count for the listing, as its profile's market cap / price — the same
    units as the historical endpoints' (native currency, and the same minor-unit price for GBp
    etc.). The profile reflects FMP's live quote, which has been right where its history wasn't
    (Hyundai Glovis ₩14.9T vs ₩105T in the history, Prudential HK HKD 245B vs 2.6T)."""
    profile = api_stocks.get_stock_profile(symbol)
    if not isinstance(profile, dict):
        return None
    cap, price = profile.get('marketCap'), profile.get('price')
    return cap / price if cap and price and cap > 0 and price > 0 else None


def _latest_run(series: dict[date, float], prices: dict[date, float]) -> list[date]:
    """The latest stretch of the series on one share count: walking back from the newest priced
    value, the dates whose implied share count (cap / price) stays within LATEST_RUN_TOLERANCE of
    the newest one. A verified count (ticker.verified_shares) is today's, so it only replaces this
    stretch: before a real change of share count — Devon Energy's merger, 621M shares in the first
    half of 2026, 1,100M after — FMP's history keeps its own values."""
    dates = sorted(d for d, v in series.items() if v and v > 0 and prices.get(d))
    if not dates:
        return []
    latest = series[dates[-1]] / prices[dates[-1]]
    run: list[date] = []
    for d in reversed(dates):
        if _ratio(series[d] / prices[d], latest) > LATEST_RUN_TOLERANCE:
            break
        run.append(d)
    return run


def clean_market_caps(
    series: dict[date, float], prices: dict[date, float] | None = None, symbol: str | None = None,
    log_outliers: bool = True, reference: float | None = None, verified_shares: float | None = None,
) -> dict[date, float]:
    """
    `series` (native currency, as FMP reports it) with its FMP glitches repaired where possible,
    else dropped. The cheap structural check runs first; the listing's reference_shares (one
    profile call) is fetched only for a series showing a glitch or worth a check anyway
    (_needs_reference), so market_cap_outliers can tell which values are the wrong ones. A
    glitch whose price is in line with the good values nearest it (the cap jumped, the price
    didn't — FMP's share count or currency is off, not its price) is repaired as that date's
    price x the implied share count (cap / price) of the REPAIR_REFERENCE_POINTS nearest good
    values — or of the reference when the whole series was off — which keeps the listing's value
    fresh through a glitch lasting months. With no price, or a price scaled along with the cap
    (pence vs pounds), the value is dropped. Logged when `symbol` is given and log_outliers.
    A caller already holding the listing's profile passes its share count as `reference`.

    With `verified_shares` (ticker.verified_shares: FMP's quote and a second source — its
    financials or the index funds — agree on a count its history doesn't; see
    refresh.verify_share_count), every value of the history's latest run (_latest_run) whose
    implied share count is SHARE_COUNT_TOLERANCE-fold off it is first set to price x
    verified_shares — on every fetch, so the daily validation compares like with like — and it
    serves as the reference.
    """
    prices = prices or {}
    if verified_shares and prices:
        off = sorted(d for d in _latest_run(series, prices)
                     if _ratio(series[d] / prices[d], verified_shares) >= SHARE_COUNT_TOLERANCE)
        if off:
            series = {**series, **{d: prices[d] * verified_shares for d in off}}
            if symbol and log_outliers:
                log.record_notice(
                    f"FMP market cap history on a wrong share count for {symbol}: {len(off)} value(s) set from "
                    f"the verified {verified_shares:,.0f} shares ({off[0]} .. {off[-1]}).")
        reference = reference or verified_shares
    if reference and prices:
        outliers = market_cap_outliers(series, prices, reference)
    else:
        outliers = market_cap_outliers(series, prices)
        if symbol and prices and (outliers or _needs_reference(series, prices)):
            reference = reference_shares(symbol)
            if reference:
                outliers = market_cap_outliers(series, prices, reference)
    if not outliers:
        return dict(series)
    cleaned = {d: v for d, v in series.items() if d not in outliers}
    good = [d for d in cleaned if cleaned[d] > 0 and prices.get(d)]
    repaired: list[date] = []
    for d in sorted(outliers):
        price = prices.get(d)
        if not price:
            continue
        if not good:
            if reference:  # the whole series was off the reference: repair from the reference itself
                cleaned[d] = price * reference
                repaired.append(d)
            continue
        nearest = sorted(good, key=lambda g: (abs((g - d).days), g))[:REPAIR_REFERENCE_POINTS]
        if _ratio(price, statistics.median(prices[g] for g in nearest)) >= EXTREME_GLITCH_FACTOR:
            continue  # the price is in another unit too (pence vs pounds): nothing to repair from
            # (a lesser gap is the market: the nearest good values can be months away)
        cleaned[d] = price * statistics.median(series[g] / prices[g] for g in nearest)
        repaired.append(d)
    if symbol and log_outliers:
        log.record_notice(
            f"FMP market cap glitch for {symbol}: {len(repaired)} value(s) repaired from the price, "
            f"{len(outliers) - len(repaired)} dropped ({min(outliers)} .. {max(outliers)})."
        )
    return cleaned


@dataclass
class Mismatch:
    ticker_id: int
    value_date: date
    field: str              # "stock_price" | "market_cap"
    stored_value: float
    fetched_value: float
    pct_diff: float


def fetch_price_and_market_cap_history(
    symbol: str, start: date, end: date, currency: str | None = None, log_outliers: bool = True,
    reference_shares: float | None = None, verified_shares: float | None = None,
) -> dict[date, tuple[float, float]] | str:
    """Parallel-fetch historical price + market cap for `symbol` over [start, end].
    Returns {date: (price, market_cap)} for weekday dates present in BOTH series,
    or the underlying error string as-is if either call fails.

    `currency` is the currency `symbol` is quoted in (see modules.ticker.util.EXCHANGE_CURRENCY)
    — market_cap is converted to USD before being returned, using that date's own historical
    FX rate (not a single blanket rate), since FMP reports it in the security's native currency
    and this is stored/compared against a USD threshold everywhere downstream. If currency is
    None/USD, no conversion is applied. If the FX rate series can't be resolved at all, this
    returns an error string (same as an unavailable price/market-cap response) so the caller
    skips/withholds rather than storing a wrong-unit value; a date with no matching FX rate
    within the lookup window is simply dropped rather than stored unconverted. A market cap
    that's an FMP glitch (market_cap_outliers) is repaired from the price, or dropped
    (clean_market_caps) — judged within [start, end], so callers validating a single date fetch
    OUTLIER_CONTEXT_DAYS of context."""
    with ThreadPoolExecutor(max_workers=2) as executor:
        prices_future = executor.submit(api_stocks.get_symbol_historic_prices, symbol, start, end)
        market_caps_future = executor.submit(api_stocks.get_stock_historic_market_cap, symbol, start, end)
        prices_raw = prices_future.result()
        market_caps_raw = market_caps_future.result()

    if isinstance(prices_raw, str):
        return prices_raw
    if isinstance(market_caps_raw, str):
        return market_caps_raw

    fx_rates = fetch_historic_usd_rates(currency, start, end)
    if isinstance(fx_rates, str):
        return fx_rates

    price_by_date = {date.fromisoformat(row["date"]): float(row["price"]) for row in prices_raw}

    # FMP glitches are judged in FMP's own units (native currency), like the profile's reference.
    native_caps = clean_market_caps(
        {date.fromisoformat(row["date"]): float(row["marketCap"]) for row in market_caps_raw},
        price_by_date, symbol, log_outliers, reference_shares, verified_shares,
    )

    market_cap_by_date: dict[date, float] = {}
    for d, raw_mc in native_caps.items():
        if not fx_rates:  # currency is None/USD - no conversion needed
            market_cap_by_date[d] = raw_mc
            continue
        rate = closest_value_for_date(fx_rates, d)
        if rate is None:
            continue  # no FX rate close enough to this date to trust - drop it, don't guess
        market_cap_by_date[d] = raw_mc * rate

    common_dates = price_by_date.keys() & market_cap_by_date.keys()
    return {
        d: (price_by_date[d], market_cap_by_date[d])
        for d in common_dates
        if d.weekday() < 5
    }


def _relative_diff(a: float, b: float) -> float:
    denom = max(abs(a), abs(b), 1e-9)
    return abs(a - b) / denom


def compare_overlap(ticker_id: int, fetched: dict[date, tuple[float, float]], stored: list[TickerValue]) -> list[Mismatch]:
    """Compares each `stored` row whose value_date is also a key in `fetched` against the
    freshly-fetched value. One Mismatch per (date, field) exceeding tolerance."""
    mismatches: list[Mismatch] = []
    for row in stored:
        if row.value_date not in fetched:
            continue
        fetched_price, fetched_market_cap = fetched[row.value_date]

        if row.stock_price is not None:
            pct_diff = _relative_diff(row.stock_price, fetched_price)
            if pct_diff > PRICE_TOLERANCE:
                mismatches.append(Mismatch(
                    ticker_id=ticker_id, value_date=row.value_date, field="stock_price",
                    stored_value=row.stock_price, fetched_value=fetched_price, pct_diff=pct_diff,
                ))

        if row.market_cap is not None:
            pct_diff = _relative_diff(row.market_cap, fetched_market_cap)
            if pct_diff > MARKET_CAP_TOLERANCE:
                mismatches.append(Mismatch(
                    ticker_id=ticker_id, value_date=row.value_date, field="market_cap",
                    stored_value=row.market_cap, fetched_value=fetched_market_cap, pct_diff=pct_diff,
                ))

    return mismatches


def mismatch_kind(mismatches: list[Mismatch]) -> str | None:
    """Whether FMP's history disagrees with ours because ours is what's off:
    - REVISION: every mismatch within REVISION_TOLERANCE - FMP revised a close (often one day);
    - BASIS: only one of price and market cap is off - the other agrees - by a steady factor
      (within BASIS_SPREAD) on every mismatching day, BASIS_MIN_POINTS or more: that stretch of
      our history is on another basis - market caps stored unconverted from their currency
      (Helsinki, Brussels, Dublin, Lisbon and New Zealand listings before those exchanges'
      currencies were known; the days stored after agree), or prices from before a split FMP
      adjusted for;
    - None: anything else (FMP's data may be what's wrong, or the symbol another security's -
      then both would be off)."""
    if all(m.pct_diff <= REVISION_TOLERANCE for m in mismatches):
        return REVISION
    if len(mismatches) < BASIS_MIN_POINTS or len({m.field for m in mismatches}) != 1:
        return None
    ratios = [m.stored_value / m.fetched_value for m in mismatches if m.stored_value and m.fetched_value]
    return BASIS if ratios and max(ratios) / min(ratios) <= BASIS_SPREAD else None


def build_mismatch_reason(mismatches: list[Mismatch]) -> str:
    parts = [
        f"{m.value_date} {m.field}: stored={m.stored_value:.4g} fetched={m.fetched_value:.4g} ({m.pct_diff:.1%} diff)"
        for m in sorted(mismatches, key=lambda m: m.value_date)[:5]
    ]
    suffix = f" (+{len(mismatches) - 5} more)" if len(mismatches) > 5 else ""
    return MISMATCH_REASON + " on " + "; ".join(parts) + suffix


VALUE_HISTORY_START = date(2026, 1, 1)  # start of stored ticker_value history (see scripts/data_fill_ticker_value_refresh.py)


def resync_value_history(
    ticker_id: int, symbol: str, currency: str | None, start: date = VALUE_HISTORY_START,
    reference_shares: float | None = None, verified_shares: float | None = None,
) -> int | str:
    """Rewrites a ticker's ticker_value history from `start` to today from FMP's historical
    endpoints, converted from `currency` and cleaned of FMP glitches — for a listing whose
    conversion currency changed (its FMP currency became known or changed), whose stored values
    were converted from the old one and would otherwise fail every validation (compare_overlap)
    until it's flagged invalid, or whose stored cap is far off its profile's (then the profile's
    share count is passed as reference_shares), or whose verified share count was set, changed
    or cleared (verified_shares: the ticker's new value). Existing rows are left untouched if FMP
    has no data. Returns rows written, or the FMP error."""
    end = date.today()
    fetched = fetch_price_and_market_cap_history(
        symbol, start, end, currency=currency, reference_shares=reference_shares, verified_shares=verified_shares)
    if isinstance(fetched, str):
        return fetched
    items = [TickerValue(ticker_id=ticker_id, value_date=d, stock_price=p, market_cap=mc) for d, (p, mc) in fetched.items()]
    _replace_range(ticker_id, start, end, items)
    return len(items)


def store_validated_ticker_value(
    ticker_id: int, symbol: str, value_date: date, exchange: str | None = None, currency: str | None = None,
    reference_shares: float | None = None, verified_shares: float | None = None,
) -> TickerValue | None:
    """market_cap is converted to USD from the listing's currency: `currency` (FMP's own
    currency for the listing — ticker.currency or the profile's) when given, else the currency
    of `exchange` (see modules.ticker.util.listing_currency). Omit both only for callers that
    can't know either (market_cap will then be stored unconverted, native-currency).

    OUTLIER_CONTEXT_DAYS of history are fetched (one call each, as before) so an FMP glitch on
    value_date is recognised as one (market_cap_outliers) and stored repaired from the price,
    or withheld when it can't be (clean_market_caps); only the last HISTORY_LOOKBACK_DAYS are
    compared with the stored values. `reference_shares` — the listing's share count from FMP's
    live quote, when the caller has it (the screener row) — is the glitch filter's reference;
    `verified_shares` (ticker.verified_shares) repairs a history on a wrong share count
    (clean_market_caps), the same way the stored history was rewritten.

    A mismatch withholds the value (and flags the ticker invalid after GRACE_PERIOD_DAYS) unless
    it's ours that's off (mismatch_kind): FMP's revised values replace the stored ones, or a
    history stored on another basis is rewritten from FMP - then the value is stored."""
    try:
        currency = tu.listing_currency(exchange, currency)
        # Not logging repaired/dropped glitches here: the same old glitch would be reported daily for months.
        fetched = fetch_price_and_market_cap_history(
            symbol, value_date - timedelta(days=OUTLIER_CONTEXT_DAYS), value_date, currency=currency, log_outliers=False,
            reference_shares=reference_shares, verified_shares=verified_shares,
        )
        if isinstance(fetched, str):
            # Not logged: no data available for this ticker/date is a routine, expected
            # outcome (not every ticker has fresh data every run) — only a ticker actually
            # being marked invalid is worth a log entry, see below.
            return None

        if value_date not in fetched:
            return None

        stored = fetch_values_for_ticker(
            ticker_id,
            value_date - timedelta(days=HISTORY_LOOKBACK_DAYS),
            value_date - timedelta(days=1),
        )
        mismatches = compare_overlap(ticker_id, fetched, stored)
        kind = mismatch_kind(mismatches) if mismatches else None

        if kind == REVISION:
            for d in {m.value_date for m in mismatches}:
                _upsert_tv(TickerValue(ticker_id=ticker_id, value_date=d, stock_price=fetched[d][0], market_cap=fetched[d][1]))
            log.record_notice(f"FMP revised ticker_id={ticker_id} ({symbol}): {build_mismatch_reason(mismatches)} - "
                              f"its values replace ours")
        elif kind == BASIS:
            written = resync_value_history(ticker_id, symbol, currency, reference_shares=reference_shares,
                                           verified_shares=verified_shares)
            log.record_notice(f"Stored history of ticker_id={ticker_id} ({symbol}) on another basis than FMP's "
                              f"({build_mismatch_reason(mismatches)}): rewritten from FMP ({written})")
            if isinstance(written, str):
                kind = None    # not rewritten: withheld as any other mismatch

        if mismatches and kind is None:
            last_good_date = fetch_latest_value_date(ticker_id)
            days_since_last_good = (value_date - last_good_date).days if last_good_date else GRACE_PERIOD_DAYS
            if days_since_last_good >= GRACE_PERIOD_DAYS:
                reason = build_mismatch_reason(mismatches)
                ticker.update_invalid(ticker_id, reason)
                log.record_notice(f"Flagged ticker_id={ticker_id} ({symbol}) invalid: {reason}")
            # else: still within its grace period - withheld silently, not logged (routine).
            return None

        price, market_cap = fetched[value_date]
        item = TickerValue(ticker_id=ticker_id, value_date=value_date, stock_price=price, market_cap=market_cap)
        _upsert_tv(item)
        return item

    except Exception as e:
        log.record_notice(f"Failed to store validated ticker_value for ticker_id={ticker_id} ({symbol}): {e}")
        return None
