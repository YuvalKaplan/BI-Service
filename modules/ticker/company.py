"""
Company-level facts derived from a company's listings (a master ticker plus its share-class /
cross-listing siblings, or a standalone ticker).

FMP reports the *whole company's* market cap on every listing, so listings are never summed.
Instead one listing — the company's primary listing — is the source of both its market cap and
its region:

  1. a listing on an exchange in the company's home country (see util.home_countries),
  2. otherwise a US-exchange listing (US-listed, foreign-domiciled companies such as Eaton or
     Medtronic, whose only listing is in the US),
  3. otherwise the master (or, before a master exists, the highest-cap listing).

Within a tier the master is preferred, then the lowest id. The market cap comes from the
first listing in that same order that has one, so a primary listing with no price data yet
falls back to the next-best listing rather than to nothing.
"""
from modules.object.ticker import Ticker
from modules.ticker import util as tu

US = 'US'
INTERNATIONAL = 'International'


def _company_country(members: list[Ticker], master_id: int | None) -> str | None:
    master = next((t for t in members if t.id == master_id), None)
    if master and master.country:
        return master.country
    return next((t.country for t in members if t.country), None)


def ordered_listings(members: list[Ticker], master_id: int | None, caps: dict[int, float] | None = None) -> list[Ticker]:
    """The company's listings, best primary-listing candidate first."""
    caps = caps or {}
    homes = tu.home_countries(_company_country(members, master_id))

    def tier(t: Ticker) -> int:
        if tu.EXCHANGE_COUNTRY.get(t.exchange or '') in homes:
            return 0
        if t.exchange in tu.US_LISTING_EXCHANGES:
            return 1
        return 2

    def key(t: Ticker):
        # Before a master exists (election), the non-home, non-US tier falls back to the
        # highest-cap listing, as the election rule always did.
        within_tier = -(caps.get(t.id) or 0.0) if (master_id is None and tier(t) == 2) else 0.0
        return (tier(t), 0 if t.id == master_id else 1, within_tier, t.id)

    return sorted(members, key=key)


def primary_listing(members: list[Ticker], master_id: int | None, caps: dict[int, float] | None = None) -> Ticker:
    return ordered_listings(members, master_id, caps)[0]


def company_market_cap(members: list[Ticker], master_id: int | None, caps: dict[int, float]) -> float | None:
    """The primary listing's market cap, else the next listing (in primary order) that has one."""
    for t in ordered_listings(members, master_id, caps):
        if caps.get(t.id):
            return caps[t.id]
    return None


def region(members: list[Ticker], master_id: int | None) -> str:
    """'US' when the primary listing trades on a US exchange — or when the company is US and we
    simply don't hold its US listing — else 'International'."""
    primary = primary_listing(members, master_id)
    if primary.exchange in tu.US_LISTING_EXCHANGES:
        return US
    homes = tu.home_countries(_company_country(members, master_id))
    has_home_listing = tu.EXCHANGE_COUNTRY.get(primary.exchange or '') in homes
    if not has_home_listing and _company_country(members, master_id) == US:
        return US
    return INTERNATIONAL
