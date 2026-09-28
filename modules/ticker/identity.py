"""
Evidence that two companies — listing groups (a master and its siblings, or a standalone
ticker) — are really one company that we failed to group. FMP's listings of one company carry
different names (a rename applied to one listing only: "GE Aerospace" vs "General Electric
Company"; "Exxon Mobil Corporation" vs "Exxonmobil Holdings Corporation"; "Midea Group" vs
"Midea Group Co., Ltd."), different ISINs (depositary receipts) and often no CIK, so the exact
CIK / name grouping in modules.ticker.master misses them — and FMP reports the whole company's
market cap on every listing, so a missed listing counts the company twice.

Used by master.link_same_company() to merge groups permanently (link_evidence), and by the
benchmark's duplicate guard (benchmark_generator) as a last safety net (duplicate_evidence).
"""
import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from modules.object.ticker import Ticker
from modules.ticker import company
from modules.ticker import util as tu

ISIN_CAP_TOLERANCE = 0.10   # a shared ISIN whose names disagree still links when the caps agree this closely
NAME_CAP_TOLERANCE = 0.02   # matching names (without a shared ISIN or CIK) link only when the caps agree this closely
CAP_MAX_DAY_GAP = 1         # caps are compared on the same date, give or take this many days

# First name tokens too common to identify a company on their own; an ISIN link between names
# starting with one of these also needs the full names_match.
_GENERIC_FIRST_TOKENS = {
    'banco', 'bank', 'grupo', 'group', 'china', 'first', 'general', 'national', 'united',
    'american', 'new', 'compagnie', 'societe', 'industrial', 'energy', 'capital', 'global',
}

# The part of a depositary receipt's FMP name that describes the receipt rather than the
# company: "Cisco Systems, Inc. Shs -CAD hedged- Canadian Depositary Receipt Repr Shs Reg S",
# "Ping An Insurance (Group) Company of China Ltd. Shs UnSp Singapore Depositary Receipt ...".
_RECEIPT_DESCRIPTOR = re.compile(
    r'\s*\b(shs|sponsored|unsponsored|unsp|american depositary|canadian depositary|singapore depositary|'
    r'global depositary|depositary|adrs?|ads|gdrs?|cdrs?|sdrs?|class [a-z])\b.*$', re.IGNORECASE)


def core_name(name: str | None) -> str:
    """The company part of a listing's name (depositary-receipt / share-class descriptors cut)."""
    return _RECEIPT_DESCRIPTOR.sub('', name or '').strip(' -,') or (name or '')


def first_word_agrees(a: str | None, b: str | None) -> bool:
    """The names agree on their first meaningful word — "Toyota Motor Corp." / "Toyota Motor
    Corporation", "Bayer AG" / "Bayer Aktiengesellschaft" — or, when that word is generic
    ("Banco", "China", …), fully match."""
    ta, tb = tu.name_tokens(a or ''), tu.name_tokens(b or '')
    if not ta or not tb or ta[0] != tb[0]:
        return False
    return ta[0] not in _GENERIC_FIRST_TOKENS or tu.names_match(a or '', b or '')


@dataclass
class CompanyView:
    """One company (listing group) as seen by the evidence rules."""
    id: int                                   # master (or standalone) ticker id
    members: list[Ticker]
    country: str | None
    name: str                                 # primary listing's name
    cik: bool                                 # any member carries a CIK
    ciks: set[str] = field(default_factory=set)
    isins: set[str] = field(default_factory=set)
    names: set[str] = field(default_factory=set)
    caps: dict[date, float] = field(default_factory=dict)   # the company's recent caps (primary listing first)
    depositary: bool = False                  # every member is a depositary receipt
    home: bool = False                        # the primary listing is in the company's home market
    tier: int = 4                             # util.market_tier of the primary listing


def build_views(
    members_of: dict[int, list[Ticker]],
    recent_caps: dict[int, dict[date, float]],
    latest_dates: dict[int, date] | None = None,
) -> dict[int, CompanyView]:
    """{company id: CompanyView}. recent_caps: {ticker_id: {date: cap}} over the last few weeks;
    a company's series is its first listing (in primary order) that has one."""
    views: dict[int, CompanyView] = {}
    for cid, ms in members_of.items():
        ordered = company.ordered_listings(ms, cid, latest_dates=latest_dates)
        primary = ordered[0]
        series = next((recent_caps[t.id] for t in ordered if recent_caps.get(t.id)), {})
        country = company.company_country(ms, cid)
        views[cid] = CompanyView(
            id=cid,
            members=ms,
            country=country,
            name=primary.name or '',
            cik=any(t.cik for t in ms),
            ciks={t.cik for t in ms if t.cik},
            isins={t.isin for t in ms if t.isin},
            names={t.name for t in ms if t.name},
            caps=series,
            depositary=all(company.is_depositary_line(t.name) for t in ms),
            home=company.is_home_listing(primary, ms, cid),
            tier=tu.market_tier(country, primary.symbol, primary.exchange),
        )
    return views


def caps_agree(a: CompanyView, b: CompanyView, tolerance: float) -> bool:
    """The two companies' caps agree within `tolerance` on the latest date both have a value
    for (± CAP_MAX_DAY_GAP days). No common date = no evidence."""
    for d in sorted(a.caps, reverse=True):
        for off in range(CAP_MAX_DAY_GAP + 1):
            for d2 in (d - timedelta(days=off), d + timedelta(days=off)):
                if d2 in b.caps:
                    ca, cb = a.caps[d], b.caps[d2]
                    return max(ca, cb) > 0 and abs(ca - cb) / max(ca, cb) <= tolerance
    return False


def same_name(a: CompanyView, b: CompanyView) -> bool:
    """Some listing of each carries the identical name (util.name_key)."""
    return bool({tu.name_key(n) for n in a.names} & {tu.name_key(n) for n in b.names} - {''})


def _names_match_any(a: CompanyView, b: CompanyView) -> bool:
    return any(tu.names_match(x, y) for x in a.names for y in b.names)


def link_evidence(a: CompanyView, b: CompanyView) -> str | None:
    """Evidence strong enough to merge two companies permanently, or None:
      - a shared ISIN, with names agreeing on their first word or caps within ISIN_CAP_TOLERANCE
        (FMP occasionally attaches another company's ISIN — Seabridge Gold carrying Santander's —
        whose cap is nowhere near),
      - same domicile, matching names and caps within NAME_CAP_TOLERANCE on the same date (FMP
        reports the whole company's cap on each listing, so two listings of one company agree),
      - a depositary receipt whose company name (core_name) matches a company of the same
        domicile (a receipt's cap is its own, so no cap check).
    Two different CIKs are two registrants and aren't linked — unless they share an ISIN under
    the identical name: a dual-listed company (Rio Tinto plc and Rio Tinto Ltd, both "Rio
    Tinto Group", both on the plc's ISIN)."""
    shared = a.isins & b.isins
    if a.ciks and b.ciks and a.ciks.isdisjoint(b.ciks):
        return f"dual listing, ISIN {min(shared)}" if shared and same_name(a, b) else None
    if shared and (first_word_agrees(a.name, b.name) or caps_agree(a, b, ISIN_CAP_TOLERANCE)):
        return f"ISIN {min(shared)}"
    if a.country and a.country == b.country:
        if _names_match_any(a, b) and caps_agree(a, b, NAME_CAP_TOLERANCE):
            return "name and market cap"
        if (a.depositary or b.depositary) and tu.names_match(core_name(a.name), core_name(b.name)):
            return "depositary receipt"
    return None


def duplicate_evidence(kept: CompanyView, candidate: CompanyView, candidate_anchored: bool) -> str | None:
    """Evidence that `candidate` duplicates an already-admitted benchmark company. Anything
    link_evidence accepts; for a candidate that isn't anchored in its home market (an unlinked
    foreign line, receipt or ADR) also a shared ISIN with agreeing names even across CIKs (Rio
    Tinto plc's ADR group vs the group holding its LSE line), or its company name matching one
    of the same domicile."""
    ev = link_evidence(kept, candidate)
    if ev or candidate_anchored:
        return ev
    shared = kept.isins & candidate.isins
    if shared and first_word_agrees(kept.name, candidate.name):
        return f"ISIN {min(shared)}"
    if kept.country and kept.country == candidate.country and tu.names_match(core_name(kept.name), core_name(candidate.name)):
        return "name"
    return None


def blocking_keys(v: CompanyView) -> set[tuple[str, str]]:
    """(country, first name token) buckets a company is compared within — every pair the name
    rules can match shares one (names_match needs every token of the shorter name matched)."""
    keys = set()
    for n in v.names | {core_name(v.name)}:
        toks = tu.name_tokens(core_name(n))
        if toks:
            keys.add((v.country or '', toks[0]))
    return keys
