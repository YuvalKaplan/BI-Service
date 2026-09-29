"""
Company-level facts derived from a company's listings (a master ticker plus its share-class /
cross-listing siblings, or a standalone ticker).

FMP reports the *whole company's* market cap on every listing, so listings are never summed.
Instead one listing — the company's primary listing — is the source of both its market cap and
its region, and is the company's master ticker. Listings are ordered (ordered_listings), best
candidate first, by:

  1. activity — a listing with no market cap in the last ACTIVE_DAYS (e.g. an old ticker left
     behind by a ticker change) ranks after every active one,
  2. share line — ordinary shares before preferred / note / depositary / when-issued / unit lines,
     and before a thin line (thin_lines: a sliver of the company's main line's turnover on the
     same country's exchanges — a unit, note or preferred FMP names like the company itself),
  3. market (util.market_tier) — the domicile country's own market, then a wider home market
     (Dutch/Luxembourg holding companies listed in Paris, Milan, …), then a US listing (US-listed,
     foreign-domiciled companies such as Eaton or Medtronic), then other countries' exchanges,
     then venues with no country of their own (OTC, LSE's International Order Book),
  4. currency — a line quoted in its exchange's own currency before one quoted in another
     (Hong Kong's RMB counters, mirrored foreign lines),
  5. the master, then (before a master exists) the highest cap, then the lowest id.

A company's cap on a given date is the first listing in that order with a value near the date
(cap_near_date); company_market_cap is the same rule applied to each listing's latest value.
"""
import re
from collections import defaultdict
from collections.abc import Container
from datetime import date, timedelta
from modules.object import ticker as ticker_obj
from modules.object import ticker_value
from modules.object.ticker import Ticker
from modules.ticker import util as tu
from modules.ticker.pricing import closest_value_for_date

US = 'US'
INTERNATIONAL = 'International'

ACTIVE_DAYS = 30  # no market cap within this many days of "as of" = inactive (e.g. a ticker left behind by a rename).
                  # Generous on purpose: many listings only get a value from the weekly screener, and a
                  # missed week or two must not flip a company's primary listing (and so its region).
THIN_TURNOVER_SHARE = 0.05  # see thin_lines. Hybrids trade 0.1-3.5% of their company's main line
                            # (Southern's units SOMN 2.3%, PPL's PPLC 3.4%); share classes kept as
                            # ordinary sit above (BRK-A 6.8%, FWONA 7.3%).

# Lines that aren't the company's common equity. FMP reports the parent company's full market
# cap on many of them (Bank of America on a Merrill Lynch note, Corteva on an EIDP preferred),
# so one admitted to the benchmark counts the company a second time.
_NON_EQUITY_NAME = re.compile(
    r'\b(pfd|preferred|pref|preference|cumulative|cum|notes?|debentures?|bonds?|obligations?|subordinated|'
    r'when[- ]issued|warrants?|rights|units|partizipationsschein|partizipsch|participation|genussschein)\b|%',
    re.IGNORECASE)
_NON_EQUITY_SYMBOL = re.compile(r'-P[A-Z]?$')  # preferred series, e.g. FITB-PM, ENB-PA
# FMP cuts some note / preferred names right after a coupon or series number, leaving a bare
# number behind a legal-form or security word: "TransCanada PipeLines Limited 6" (its 6.50%
# notes), "KKR Group Finance Co. IX LLC 4.", "Southern Company (The) Series 2", "... PARRS A 2029",
# "... PFD 1". A company named with a number ("Phillips 66") doesn't have such a word before it.
_TRUNCATED_COUPON_NAME = re.compile(
    r'\b(limited|ltd|llc|inc|corp|corporation|company|co|series|nts?|pfd|due|[a-z])\.?\s+\d+(\.\d*)?$', re.IGNORECASE)
_DEPOSITARY_NAME = re.compile(r'\b(depositary|depository|adrs?|ads|gdrs?|cdrs?|sdrs?|edrs?)\b', re.IGNORECASE)

_KRX_EXCHANGES = {'KSC', 'KOE'}


def is_non_equity_line(symbol: str | None, name: str | None, exchange: str | None, free_float: float | None = None) -> bool:
    """Preferred, note/bond, when-issued, warrant/right, unit or participation-certificate line
    rather than the company's ordinary shares. Korean preferred shares carry no marker in their
    FMP name; by KRX convention they're the codes not ending in 0 (005935 vs common 005930, 02826K).
    FMP names some notes exactly like their issuer (Algonquin's AQNB, Brookfield Renewable's BEPI)
    but reports no equity float for them: a listing's `free_float` (ticker.free_float) of 0 marks
    one — the only sign when the issuer's own shares aren't registered to compare turnover with."""
    if exchange in _KRX_EXCHANGES and symbol and not symbol.endswith('0'):
        return True
    if free_float == 0:
        return True
    return bool((name and (_NON_EQUITY_NAME.search(name) or _TRUNCATED_COUPON_NAME.search(name)))
                or (symbol and _NON_EQUITY_SYMBOL.search(symbol)))


def is_depositary_line(name: str | None) -> bool:
    """A depositary receipt (ADR/ADS, GDR, and the Canadian/Singapore CDRs/SDRs of foreign
    companies) as far as its FMP name says so — many ADRs' names carry no marker."""
    return bool(name and _DEPOSITARY_NAME.search(name))


def is_secondary_line(t: Ticker) -> bool:
    return is_non_equity_line(t.symbol, t.name, t.exchange, t.free_float) or is_depositary_line(t.name)


def turnover_group_key(t: Ticker) -> tuple:
    """The lines a listing's turnover is compared with (thin_lines): the same country's exchanges
    in the same currency — NYSE, NASDAQ and AMEX together, NSE with BSE, XETRA with Frankfurt —
    or, on a venue with no country of its own (OTC, LSE's order book), that venue only."""
    return (tu.listing_country(t.symbol, t.exchange) or t.exchange, tu.listing_currency(t.exchange, t.currency))


def thin_lines(members: list[Ticker]) -> set[int]:
    """Ids of the company's listings trading under THIN_TURNOVER_SHARE of its busiest ordinary
    line on the same country's exchanges, in the same currency (turnover_group_key,
    ticker.average_turnover). FMP names some unit, note and preferred lines exactly like the
    company and stamps its whole market cap on them — Southern Company's 2025 corporate units
    SOMN, ANZ's capital notes AN3PJ, Comcast's exchangeable debentures CCZ (on NYSE, against the
    shares on NASDAQ) — so neither name nor symbol tells them from the ordinary shares, but their
    turnover does. A thinly traded share class (Carlsberg A, McCormick's voting shares) or venue
    (BSE against NSE) ranks behind the main one too. A line with no turnover recorded is never thin."""
    busiest: dict[tuple, float] = {}
    for t in members:
        if t.average_turnover and not is_secondary_line(t):
            key = turnover_group_key(t)
            busiest[key] = max(busiest.get(key, 0.0), t.average_turnover)
    return {t.id for t in members
            if t.average_turnover is not None
            and t.average_turnover < THIN_TURNOVER_SHARE * busiest.get(turnover_group_key(t), 0.0)}


def is_foreign_currency_line(t: Ticker) -> bool:
    """Quoted in a currency other than its exchange's (when FMP's currency for it is known)."""
    own = tu.normalize_currency(t.currency)
    return bool(own) and own != tu.currency_for_exchange(t.exchange)


def _company_country(members: list[Ticker], master_id: int | None) -> str | None:
    master = next((t for t in members if t.id == master_id), None)
    if master and master.country:
        return master.country
    return next((t.country for t in members if t.country), None)


def company_country(members: list[Ticker], master_id: int | None) -> str | None:
    return _company_country(members, master_id)


def company_id(t: Ticker, known_ids: Container[int]) -> int:
    """The id a listing's company is keyed by: its master's, or its own when it has no master
    or the master isn't among known_ids (e.g. an invalid master filtered out)."""
    mid = t.master_ticker_id
    return mid if mid is not None and mid in known_ids else t.id


def listing_index(listings: list[Ticker], full_symbol: dict[int, str]) -> tuple[dict, dict, dict]:
    """Listing ids by FMP full symbol, ISIN and CUSIP - how an FMP fund holding line (asset,
    isin, securityCusip) is matched to our listings, in that order."""
    by_sym, by_isin, by_cusip = defaultdict(set), defaultdict(set), defaultdict(set)
    for t in listings:
        by_sym[full_symbol[t.id]].add(t.id)
        if t.isin:
            by_isin[t.isin].add(t.id)
        if t.cusip:
            by_cusip[t.cusip].add(t.id)
    return by_sym, by_isin, by_cusip


def ordered_listings(
    members: list[Ticker],
    master_id: int | None,
    caps: dict[int, float] | None = None,
    latest_dates: dict[int, date] | None = None,
    as_of: date | None = None,
    active_days: int = ACTIVE_DAYS,
) -> list[Ticker]:
    """The company's listings, best primary-listing candidate first (see module docstring).
    latest_dates ({ticker_id: latest ticker_value date}) enables the activity rule; without
    it every listing counts as active."""
    caps = caps or {}
    country = _company_country(members, master_id)
    cutoff = (as_of or date.today()) - timedelta(days=active_days)
    thin = thin_lines(members)

    def inactive(t: Ticker) -> int:
        if latest_dates is None:
            return 0
        d = latest_dates.get(t.id)
        return 0 if d is not None and d >= cutoff else 1

    def key(t: Ticker):
        tier = tu.market_tier(country, t.symbol, t.exchange)
        # Before a master exists (election), the foreign/OTC tiers fall back to the
        # highest-cap listing, as the election rule always did.
        within_tier = -(caps.get(t.id) or 0.0) if (master_id is None and tier >= 3) else 0.0
        return (inactive(t), 1 if is_secondary_line(t) or t.id in thin else 0, tier, 1 if is_foreign_currency_line(t) else 0,
                0 if t.id == master_id else 1, within_tier, t.id)

    return sorted(members, key=key)


def primary_listing(
    members: list[Ticker],
    master_id: int | None,
    caps: dict[int, float] | None = None,
    latest_dates: dict[int, date] | None = None,
    active_days: int = ACTIVE_DAYS,
) -> Ticker:
    return ordered_listings(members, master_id, caps, latest_dates, active_days=active_days)[0]


def is_home_listing(t: Ticker, members: list[Ticker], master_id: int | None) -> bool:
    """Whether `t` trades in its company's home market (util.market_tier 0 or 1)."""
    return tu.market_tier(_company_country(members, master_id), t.symbol, t.exchange) <= 1


def company_market_cap(
    members: list[Ticker],
    master_id: int | None,
    caps: dict[int, float],
    latest_dates: dict[int, date] | None = None,
) -> float | None:
    """The primary listing's latest market cap, else the next listing (in primary order) that has one."""
    for t in ordered_listings(members, master_id, caps, latest_dates):
        if caps.get(t.id):
            return caps[t.id]
    return None


def cap_near_date(
    members: list[Ticker],
    master_id: int | None,
    values: dict[int, dict[date, float]],
    target: date,
    window: int,
) -> float | None:
    """The company's market cap as of `target`: the value closest to `target` (within ±window
    days) of the first listing, in primary order, that has one. Activity is judged as of
    `target`, so a ticker that has since been renamed still counts on its own dates."""
    latest_dates = {tid: max(d for d in vals if d <= target + timedelta(days=window))
                    for tid, vals in values.items() if any(d <= target + timedelta(days=window) for d in vals)}
    for t in ordered_listings(members, master_id, latest_dates=latest_dates, as_of=target):
        series = values.get(t.id)
        if series:
            v = closest_value_for_date(series, target, window_days=window)
            if v:
                return v
    return None


def company_caps_as_of(master_ids: list[int], target: date, window: int) -> dict[int, float]:
    """{master_id: the company's market cap as of `target`} (cap_near_date over all its listings'
    stored values within ±window days). Masters with no listing value near the date are absent —
    callers fall back to the stored company_market_cap."""
    if not master_ids:
        return {}
    groups = ticker_obj.fetch_group_members(list(master_ids))
    ids = [t.id for ms in groups.values() for t in ms]
    values = ticker_value.fetch_market_caps_between(ids, target - timedelta(days=window), target + timedelta(days=window))
    out: dict[int, float] = {}
    for mid, members in groups.items():
        if len(members) < 2:
            continue
        cap = cap_near_date(members, mid, {t.id: values[t.id] for t in members if t.id in values}, target, window)
        if cap:
            out[mid] = cap
    return out


def region(
    members: list[Ticker],
    master_id: int | None,
    latest_dates: dict[int, date] | None = None,
) -> str:
    """'US' when the primary listing trades on a US exchange — or when a US company's primary
    listing is on a venue with no country of its own (OTC, LSE's order book) — else
    'International'. A US company whose best listing is on a foreign exchange is either an
    unlinked foreign line or a wrong FMP domicile (CVC Capital Partners), never a US listing."""
    primary = primary_listing(members, master_id, latest_dates=latest_dates)
    if primary.exchange in tu.US_LISTING_EXCHANGES and not tu.is_iob_line(primary.symbol, primary.exchange):
        return US
    if tu.listing_country(primary.symbol, primary.exchange) is None and _company_country(members, master_id) == US:
        return US
    return INTERNATIONAL
