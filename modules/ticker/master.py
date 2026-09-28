import log
from datetime import date, datetime, timedelta
from modules.object import ticker, ticker_value
from modules.object.ticker import Ticker
from modules.ticker import company, identity
from modules.ticker import util as tu

# A master is realigned to a better primary listing only when that listing has a market cap
# within this many days — longer than company.ACTIVE_DAYS, so a listing that misses a few weekly
# screener runs doesn't swap the company's id back and forth.
REALIGN_ACTIVE_DAYS = 90
RECENT_CAP_DAYS = 21  # how far back link_same_company looks for caps to compare on a common date


def _elect_master(
    candidate_ids: list[int],
    tickers_by_id: dict[int, Ticker],
    market_caps: dict[int, float],
) -> int:
    """First-election rule: the company's primary listing (active ordinary shares, home-market
    listing, else a US listing — see modules.ticker.company), else the highest-cap listing,
    then lowest id."""
    members = [tickers_by_id[tid] for tid in candidate_ids]
    latest_dates = {tid: d for tid, (d, _cap) in ticker_value.fetch_latest_values(candidate_ids).items()}
    return company.primary_listing(members, None, market_caps, latest_dates).id


def _sync_group(
    member_ids: list[int],
    tickers_by_id: dict[int, Ticker],
    market_caps: dict[int, float],
) -> list[tuple[int, int]]:
    """
    Given a candidate group (all sharing a CIK, or all sharing a normalized name), returns
    the (ticker_id, master_ticker_id) assignments still needed. If any member already has an
    established master, every other member is (re)pointed at that same master (market-cap
    moves never re-elect it); only a group with no existing master calls _elect_master.
    align_masters_to_primary() later moves a master to a better primary listing, if any.
    """
    existing_masters = {
        mid
        for tid in member_ids
        if (mid := tickers_by_id[tid].master_ticker_id) is not None
    }
    master_id = min(existing_masters) if existing_masters else _elect_master(member_ids, tickers_by_id, market_caps)

    return [
        (tid, master_id)
        for tid in member_ids
        if tid != master_id and tickers_by_id[tid].master_ticker_id != master_id
    ]


def _name_key(name: str | None) -> str:
    return tu.name_key(name)


def _tied(a: Ticker, b: Ticker) -> bool:
    """Evidence that keeps two already-linked listings together: same CIK; same ISIN with names
    agreeing on their first word or the same domicile; identical normalized name; or (same
    domicile) matching names or a depositary receipt's company name. Looser than what creates a
    link (identity.link_evidence also wants the caps to agree), so a link made on a week the
    caps agreed isn't undone on a week one listing has no fresh cap."""
    if a.cik and b.cik and a.cik == b.cik:
        return True
    if a.isin and b.isin and a.isin == b.isin and (identity.first_word_agrees(a.name, b.name) or (a.country and a.country == b.country)):
        return True
    if bool(_name_key(a.name)) and _name_key(a.name) == _name_key(b.name):
        return True
    if a.country and a.country == b.country:
        if tu.names_match(a.name or '', b.name or ''):
            return True
        if tu.names_match(identity.core_name(a.name), identity.core_name(b.name)):
            return True
    return False


def unlink_stale_master_tickers() -> int:
    """
    Unlinks a sibling that no longer belongs with its company — e.g. after a profile refresh
    corrected a CIK, ISIN or name. A sibling whose CIK differs from its master's is unlinked
    (different registrants) unless the company is dual-listed (same ISIN and name as another of
    its listings); otherwise it stays linked while it's tied (_tied) to its
    master or to any other member of the group, so a listing linked through another listing
    (by ISIN, say) isn't unlinked just because its spelling differs from the master's. Run
    before sync_master_tickers() so an unlinked ticker can be regrouped correctly in the same
    run. Masters themselves are never re-elected; only sibling links are removed.
    Returns the number of tickers unlinked.
    """
    siblings = ticker.fetch_linked_siblings()
    if not siblings:
        return 0
    master_of = {s.id: mid for s in siblings if (mid := s.master_ticker_id) is not None}
    masters_by_id = {t.id: t for t in ticker.fetch_by_ids(sorted(set(master_of.values())))}
    members: dict[int, list[Ticker]] = {mid: [m] for mid, m in masters_by_id.items()}
    for s in siblings:
        members.setdefault(master_of[s.id], []).append(s)

    def dual_listed(s: Ticker) -> bool:
        # A dual-listed company's two registrants (Rio Tinto plc / Ltd): another listing of the
        # group carries the same ISIN under the identical name (see identity.link_evidence).
        return any(o.id != s.id and s.isin and o.isin == s.isin and _name_key(o.name) == _name_key(s.name)
                   for o in members[master_of[s.id]])

    stale: list[int] = []
    for s in siblings:
        m = masters_by_id.get(master_of[s.id])
        cik_conflict = m is not None and s.cik and m.cik and s.cik != m.cik and not dual_listed(s)
        tied = m is not None and not cik_conflict and any(o.id != s.id and _tied(s, o) for o in members[master_of[s.id]])
        if not tied:
            stale.append(s.id)
            log.record_notice(
                f"Unlinked ticker_id={s.id} ({s.symbol}, {s.name}) from master ticker_id={s.master_ticker_id}"
                + (f" ({m.symbol}, {m.name})" if m else "") + ": no longer tied to the company by CIK, ISIN or name."
            )

    ticker.clear_master_ticker_bulk(stale)
    return len(stale)


def repair_master_chains() -> int:
    """
    Makes every link point directly at a root master: a ticker whose master itself has a master
    is re-pointed at the end of the chain, and a cycle (A -> B -> A) is broken by electing one
    of its members as the master and clearing that member's own link. Run before and after
    sync_master_tickers() so grouping always starts from, and leaves, a flat one-level structure.
    Returns the number of tickers re-pointed or cleared.
    """
    master_of: dict[int, int | None] = {s.id: s.master_ticker_id for s in ticker.fetch_linked_siblings()}
    if not master_of:
        return 0

    fixes: dict[int, int | None] = {}
    for start in list(master_of):
        path: list[int] = []
        node = start
        while (next_node := master_of.get(node)) is not None and node not in path:
            path.append(node)
            node = next_node
        if node in path:  # cycle: elect a master among the cycle members
            cycle = path[path.index(node):]
            tickers_by_id = {t.id: t for t in ticker.fetch_by_ids(cycle)}
            caps = ticker_value.fetch_latest_market_caps(cycle)
            root = _elect_master(cycle, tickers_by_id, caps)
            master_of[root] = None
            fixes[root] = None
            log.record_notice(f"Master cycle {cycle} broken: ticker_id={root} elected master.")
            node = root
        for tid in path:
            if tid != node and master_of.get(tid) != node:
                master_of[tid] = node
                fixes[tid] = node

    ticker.update_master_ticker_bulk(list(fixes.items()))
    if fixes:
        log.record_status(f"Ticker master repair: {len(fixes)} link(s) flattened.")
    return len(fixes)


def sync_master_tickers() -> int:
    """
    Idempotent. Pass A groups tickers sharing a CIK. Pass B handles tickers with no CIK
    (typically non-US listings) by normalized company name: a name that matches exactly one CIK
    company joins that company's master — this is how a US company's foreign listings (e.g.
    Alphabet on XETRA) end up under the US master, even when they had already formed a group
    of their own. Other no-CIK names group among themselves (_sync_group). Names are compared
    by util.name_key (case, accents and punctuation ignored, legal suffixes kept).
    Returns the number of ticker rows whose master_ticker_id was newly assigned/changed.
    """
    assignments: list[tuple[int, int]] = []

    # Pass A: CIK-based groups.
    cik_groups = ticker.fetch_cik_groups()
    cik_ids = sorted({tid for _cik, ids in cik_groups for tid in ids})
    tickers_by_id = {t.id: t for t in ticker.fetch_by_ids(cik_ids)} if cik_ids else {}
    market_caps = ticker_value.fetch_latest_market_caps(cik_ids) if cik_ids else {}
    for _cik, member_ids in cik_groups:
        assignments.extend(_sync_group(member_ids, tickers_by_id, market_caps))
    pass_a = dict(assignments)

    # Name index of CIK companies: name -> the company's master (after Pass A's assignments).
    cik_company_by_name: dict[str, set[int]] = {}
    for t in ticker.fetch_cik_tickers():
        key = _name_key(t.name)
        if key:
            cik_company_by_name.setdefault(key, set()).add(pass_a.get(t.id) or t.master_ticker_id or t.id)

    # Pass B: tickers that still have no CIK, grouped by normalized name.
    no_cik_tickers = ticker.fetch_no_cik_tickers()
    name_groups: dict[str, list[int]] = {}
    no_cik_by_id: dict[int, Ticker] = {}
    for t in no_cik_tickers:
        no_cik_by_id[t.id] = t
        name_groups.setdefault(_name_key(t.name), []).append(t.id)

    merged = 0
    ambiguous: list[str] = []
    local_groups: list[list[int]] = []
    for key, member_ids in name_groups.items():
        companies = cik_company_by_name.get(key)
        if companies and len(companies) == 1:
            target = next(iter(companies))
            moves = [(tid, target) for tid in member_ids if tid != target and no_cik_by_id[tid].master_ticker_id != target]
            assignments.extend(moves)
            merged += len(moves)
        elif companies:
            ambiguous.append(key)
        elif len(member_ids) > 1:
            local_groups.append(member_ids)

    name_ids = [tid for ids in local_groups for tid in ids]
    name_market_caps = ticker_value.fetch_latest_market_caps(name_ids) if name_ids else {}
    for member_ids in local_groups:
        assignments.extend(_sync_group(member_ids, no_cik_by_id, name_market_caps))

    ticker.update_master_ticker_bulk(assignments)
    if ambiguous:
        log.record_notice(f"Ticker master sync: {len(ambiguous)} no-CIK name(s) match several CIK companies, left unlinked: {ambiguous[:20]}")
    log.record_status(f"Ticker master sync: {len(assignments)} assignment(s), {merged} into CIK companies by name.")
    return len(assignments)


def _follow(moved: dict[int, int], c: int) -> int:
    while c in moved:
        c = moved[c]
    return c


# Result of the latest link_same_company() / align_masters_to_primary() run, for build_master_groups_report.
LAST_LINKS: list[str] = []
LAST_REJECTIONS: list[str] = []
LAST_REALIGNED: list[str] = []


def _describe(t: Ticker) -> str:
    return f"{t.symbol}:{t.exchange} [{t.name}]"


def _current_companies() -> tuple[list[Ticker], dict[int, list[Ticker]]]:
    """Valid tickers and {company id: its listings}, company id = master (or own) id."""
    all_t = [t for t in ticker.fetch_all() if not t.invalid]
    by_id = {t.id: t for t in all_t}
    members: dict[int, list[Ticker]] = {}
    for t in all_t:
        members.setdefault(company.company_id(t, by_id), []).append(t)
    return all_t, members


def link_same_company() -> int:
    """
    Merges companies (listing groups) that identity.link_evidence says are one company: a
    shared ISIN (names agreeing on their first word, or caps within 10% — "GE Aerospace" /
    "General Electric Company", "Exxon Mobil" / "Exxonmobil Holdings"), matching names of the
    same domicile with caps within 2% on the same date (Midea's A and H shares, "AXIA Energia" /
    "AXIA Energia S.A."), or a depositary receipt whose company name matches (Cisco's Canadian
    DR). The target is the company with a CIK, then one whose primary listing is in its home
    market, then the most listings, then the lowest id; every listing of the other company is
    re-pointed to it (align_masters_to_primary then picks the right master). Two different CIKs
    are never merged; ISIN matches rejected by the guard are kept for the report.
    Returns the number of listings re-pointed.
    """
    global LAST_LINKS, LAST_REJECTIONS
    all_t, members = _current_companies()
    ids = [t.id for t in all_t]
    today = date.today()
    recent = ticker_value.fetch_market_caps_between(ids, today - timedelta(days=RECENT_CAP_DAYS), today)
    latest_dates = {tid: max(vals) for tid, vals in recent.items() if vals}
    views = identity.build_views(members, recent, latest_dates)

    # Candidate pairs: companies sharing an ISIN, or a (domicile, first name word) bucket.
    buckets: dict[tuple[str, str], set[int]] = {}
    for cid, v in views.items():
        for isin in v.isins:
            buckets.setdefault(('isin', isin), set()).add(cid)
        for key in identity.blocking_keys(v):
            buckets.setdefault(key, set()).add(cid)

    def rank(c: int):
        v = views[c]
        return (v.cik, v.home, len(v.members), -c)

    moved: dict[int, int] = {}
    links: list[str] = []
    rejections: list[str] = []
    seen_pairs: set[tuple[int, int]] = set()
    for key, cids in sorted(buckets.items()):
        if len(cids) < 2 or len(cids) > 200:   # a huge bucket is a generic word, not a company
            continue
        for a in sorted(cids):
            for b in sorted(cids):
                if b <= a or (a, b) in seen_pairs:
                    continue
                seen_pairs.add((a, b))
                ca, cb = _follow(moved, a), _follow(moved, b)
                if ca == cb:
                    continue
                va, vb = views[ca], views[cb]
                evidence = identity.link_evidence(va, vb)
                if evidence is None:
                    if key[0] == 'isin' and not (va.ciks and vb.ciks and va.ciks.isdisjoint(vb.ciks)):
                        rejections.append(f"{key[1]}: {va.name} ({va.country}) vs {vb.name} ({vb.country}) — names and caps disagree")
                    continue
                target, other = (ca, cb) if rank(ca) >= rank(cb) else (cb, ca)
                vt, vo = views[target], views[other]
                vt.members.extend(vo.members)
                vt.isins |= vo.isins
                vt.names |= vo.names
                vt.ciks |= vo.ciks
                vt.cik = vt.cik or vo.cik
                moved[other] = target
                links.append(f"{evidence}: {vo.name} ({vo.country}, {len(vo.members)} listing(s)) -> {vt.name}")

    by_id = {t.id: t for t in all_t}
    targets = {_follow(moved, cid) for cid in moved}
    # Every listing of a merged-away company (its own row included) points at the final target.
    assignments = {
        t.id: _follow(moved, cid)
        for cid in moved
        for t in views[cid].members
    }
    assignments = [(tid, mid) for tid, mid in assignments.items() if tid != mid and by_id[tid].master_ticker_id != mid]
    # A target is a company id: its own row must not point at a master (e.g. a stale link to an
    # invalid ticker).
    ticker.clear_master_ticker_bulk([tid for tid in targets if by_id[tid].master_ticker_id is not None])
    ticker.update_master_ticker_bulk(assignments)
    for r in rejections:
        log.record_notice(f"ISIN match rejected — {r}")
    log.record_status(f"Ticker same-company link: {len(moved)} compan(ies) merged ({len(assignments)} listing(s)), {len(rejections)} ISIN match(es) rejected.")
    LAST_LINKS, LAST_REJECTIONS = links, rejections
    return len(assignments)


def align_masters_to_primary() -> int:
    """
    Makes every company's master its primary listing (company.primary_listing: active ordinary
    shares in the home market, else a US listing, …), re-pointing the whole group when a better
    listing than the current master exists — e.g. Bank of America's master 0Q16 (LSE order
    book) becomes BAC (NYSE), TSMC's becomes 2330 (Taiwan). The current master wins ties, and a
    listing needs a market cap within REALIGN_ACTIVE_DAYS to take over, so masters don't swap
    back and forth. Stored ids referring to the old master (benchmark_holding, best_idea,
    fund_holding) still resolve to the company through COALESCE(master_ticker_id, id).
    Returns the number of companies realigned.
    """
    global LAST_REALIGNED
    all_t = ticker.fetch_all()
    by_id = {t.id: t for t in all_t}
    members: dict[int, list[Ticker]] = {}
    for t in all_t:
        members.setdefault(company.company_id(t, by_id), []).append(t)
    grouped_ids = [t.id for ms in members.values() if len(ms) > 1 for t in ms]
    latest = ticker_value.fetch_latest_values(grouped_ids)
    caps = {tid: cap for tid, (_d, cap) in latest.items()}
    latest_dates = {tid: d for tid, (d, _cap) in latest.items()}

    clears: list[int] = []
    assignments: list[tuple[int, int]] = []
    realigned: list[str] = []
    for root, ms in members.items():
        if len(ms) < 2:
            continue
        candidates = [t for t in ms if not t.invalid] or ms
        if root not in {t.id for t in candidates}:
            candidates = candidates + [by_id[root]]
        primary = company.primary_listing(candidates, root, caps, latest_dates, active_days=REALIGN_ACTIVE_DAYS)
        if primary.id == root:
            continue
        clears.append(primary.id)
        assignments.extend((t.id, primary.id) for t in ms if t.id != primary.id)
        realigned.append(f"{_describe(by_id[root])} -> {_describe(primary)}")

    ticker.clear_master_ticker_bulk(clears)
    ticker.update_master_ticker_bulk(assignments)
    if realigned:
        log.record_status(f"Ticker master realignment: {len(realigned)} compan(ies) moved to their primary listing.")
    LAST_REALIGNED = realigned
    return len(realigned)


def refresh_company_data() -> int:
    """
    For every company, finds its primary listing (modules.ticker.company) and persists:
      - company_market_cap on the master: the primary listing's latest market cap. FMP reports
        the whole company's cap on every listing, so listings are never summed.
      - region on every listing (standalone tickers included): 'US' | 'International'.
    Returns the number of masters whose company cap was set.
    """
    all_tickers = ticker.fetch_all()
    by_id = {t.id: t for t in all_tickers}
    members_of: dict[int, list[Ticker]] = {}
    for t in all_tickers:
        members_of.setdefault(company.company_id(t, by_id), []).append(t)

    grouped_ids = [t.id for ms in members_of.values() if len(ms) > 1 for t in ms]
    latest = ticker_value.fetch_latest_values(grouped_ids) if grouped_ids else {}
    caps = {tid: cap for tid, (_d, cap) in latest.items()}
    latest_dates = {tid: d for tid, (d, _cap) in latest.items()}

    cap_updates: list[tuple[int, float | None]] = []
    region_updates: list[tuple[int, str]] = []
    for root, members in members_of.items():
        region = company.region(members, root, latest_dates if len(members) > 1 else None)
        region_updates.extend((t.id, region) for t in members)
        if len(members) > 1:
            cap_updates.append((root, company.company_market_cap(members, root, caps, latest_dates)))

    ticker.update_company_market_cap_bulk(cap_updates)
    ticker.update_region_bulk(region_updates)
    cleared = ticker.clear_stale_company_market_caps()
    log.record_status(
        f"Company data refresh: {len(cap_updates)} company cap(s), region checked on {len(region_updates)} ticker(s), {cleared} stale cap(s) cleared."
    )
    return len(cap_updates)


def sync_masters_and_company_data() -> tuple[int, int, int]:
    """Returns (links_added, links_removed, company_caps_refreshed). Unlinking runs first so a
    ticker whose CIK/name changed can be regrouped correctly in the same run; chains/cycles are
    flattened before and after grouping; masters are then moved to each company's primary
    listing, before the company cap and region are derived from it."""
    unlinked = unlink_stale_master_tickers()
    repair_master_chains()
    masters_updated = sync_master_tickers()
    masters_updated += repair_master_chains()
    masters_updated += link_same_company()
    masters_updated += repair_master_chains()
    masters_updated += align_masters_to_primary()
    caps_updated = refresh_company_data()
    return masters_updated, unlinked, caps_updated


def build_master_groups_report(masters_updated: int = 0, caps_updated: int = 0, unlinked: int = 0) -> str:
    """
    Human-readable summary of every current master-ticker group. CIK matches (the reliable,
    high-volume case) are only counted, not listed individually; name matches (the fallback
    heuristic, more worth double-checking) are each listed with master + sibling symbols and
    company names. Shared by scripts/data_fill_master_tickers.py (printed + written to a
    report file) and scripts/sim_prep_data.py (printed only).

    Detection method isn't persisted anywhere, so it's inferred here: if the master and every
    sibling currently share the same non-empty cik, it's a CIK match (sync_master_tickers()'s
    Pass A); otherwise it's a name match (Pass B — the only other way a group can form).
    """
    groups = ticker.fetch_master_groups()  # [(master_id, [sibling_ids...]), ...]

    lines = [
        "# Master Ticker Groups Report",
        "",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"This run: {masters_updated} new link(s), {unlinked} unlinked, {caps_updated} company cap(s) refreshed.",
        "",
    ]

    if not groups:
        lines.append("No master ticker groups found.")
        return "\n".join(lines)

    all_ids = sorted({tid for master_id, sibling_ids in groups for tid in [master_id] + sibling_ids})
    tickers_by_id = {t.id: t for t in ticker.fetch_by_ids(all_ids)}

    def _describe(sid: int) -> str:
        t = tickers_by_id.get(sid)
        if not t:
            return f"id={sid}"
        return f"{t.symbol} ({t.name or 'unknown'}, id={sid})"

    us_companies = 0
    intl_companies = 0
    cik_client_tickers = 0
    name_client_tickers = 0
    name_lines: list[str] = []

    for master_id, sibling_ids in sorted(groups, key=lambda g: g[0]):
        master_ticker = tickers_by_id.get(master_id)

        if master_ticker and master_ticker.exchange in ticker.US_EXCHANGES:
            us_companies += 1
        else:
            intl_companies += 1

        member_tickers = [t for t in [master_ticker] + [tickers_by_id.get(sid) for sid in sibling_ids] if t]
        member_ciks = [t.cik for t in member_tickers]
        matched_by_cik = bool(member_ciks) and all(member_ciks) and len(set(member_ciks)) == 1

        if matched_by_cik:
            cik_client_tickers += len(sibling_ids)
            continue

        name_client_tickers += len(sibling_ids)
        master_symbol = master_ticker.symbol if master_ticker else f"id={master_id}"
        master_name = master_ticker.name if master_ticker and master_ticker.name else "unknown"
        cap = master_ticker.company_market_cap if master_ticker else None
        cap_str = f"${cap:,.0f}" if cap else "n/a"
        sibling_desc = ", ".join(_describe(sid) for sid in sorted(sibling_ids))
        name_lines.append(
            f"- **{master_symbol}** ({master_name}, id={master_id}, "
            f"company_market_cap={cap_str}) <- {sibling_desc}"
        )

    lines.append("## Summary")
    lines.append("")
    lines.append(f"Companies: {len(groups)} ({us_companies} US, {intl_companies} International)")
    lines.append(f"Client tickers matched by CIK: {cik_client_tickers} (not listed individually — CIK matching is reliable)")
    lines.append(f"Client tickers matched by name: {name_client_tickers}")
    lines.append("")
    lines.append(f"## Matched by name: {len(name_lines)} group(s)")
    lines.append("")
    lines.extend(name_lines if name_lines else ["None."])
    lines.append("")
    lines.append(f"## Companies merged this run (ISIN, name + market cap, depositary receipt): {len(LAST_LINKS)}")
    lines.append("")
    lines.extend([f"- {x}" for x in LAST_LINKS] or ["None."])
    lines.append("")
    lines.append(f"## ISIN matches rejected: {len(LAST_REJECTIONS)} (names and caps disagree — review: renames, or a wrong ISIN at FMP)")
    lines.append("")
    lines.extend([f"- {x}" for x in LAST_REJECTIONS] or ["None."])
    lines.append("")
    lines.append(f"## Masters moved to the company's primary listing this run: {len(LAST_REALIGNED)}")
    lines.append("")
    lines.extend([f"- {x}" for x in LAST_REALIGNED] or ["None."])

    return "\n".join(lines)
