import log
from datetime import datetime
from modules.core import api_stocks
from modules.object import ticker, ticker_value
from modules.object.ticker import Ticker
from modules.ticker import company
from modules.ticker import util as tu
from modules.ticker.resolver import TickerResolver


def refresh_ticker_profiles(include_invalid: bool = False) -> tuple[int, int, int]:
    """
    Refreshes full ticker profile data (isin/cusip/cik/name/industry/sector/country/currency/
    is_actively_trading) from FMP for every ticker whose profile hasn't been checked within
    the last week (ticker.fetch_stale_tickers) — the single shared implementation used by
    scripts/data_fill_ticker_profile.py, scripts/sim_prep_data.py, and the live Tue-Sat cron
    step, so all three go over the exact same ticker list rather than each having their own
    narrower variant (e.g. the old cik-only backfill).

    A ticker is flagged invalid if its profile turns out to be crypto, a fund/ETF, or no
    longer actively trading — same detection as the original data_fill_ticker_profile.py.
    A ticker whose profile lookup itself returns an invalid/error response is also marked
    invalid (rather than merely logged and retried forever), since fetch_stale_tickers()
    already excludes already-invalid tickers by default — pass include_invalid=True to retry
    them too (a ticker that checks out fine on retry has its invalid flag cleared).

    Returns (total_checked, updated, marked_invalid).
    """
    resolver = TickerResolver(TickerResolver.POPULATE_TICKER)
    tickers = ticker.fetch_stale_tickers(include_invalid=include_invalid)

    updated = 0
    marked_invalid = 0

    for t in tickers:
        full_symbol = resolver.get_full_symbol(t)
        profile = api_stocks.get_stock_profile(full_symbol)
        if not isinstance(profile, dict):
            log.record_notice(f"Ticker profile refresh: no profile for '{full_symbol}': {profile}")
            ticker.update_invalid(t.id, "Profile lookup failed")
            marked_invalid += 1
            continue

        exchange = profile.get('exchange')
        name = profile.get('companyName')
        is_active = profile.get('isActivelyTrading')

        invalid_reason = None
        if exchange == 'CRYPTO':
            invalid_reason = 'Crypto'
        elif not name or tu.is_unwanted_names(name):
            invalid_reason = 'Fund or ETF'
        elif is_active is not None and not is_active:
            invalid_reason = 'Not actively trading'

        updated_ticker = Ticker(
            id=t.id,
            symbol=t.symbol,
            isin=profile.get('isin') or t.isin,
            cusip=profile.get('cusip') or t.cusip,
            cik=profile.get('cik') or t.cik,
            name=name or t.name,
            exchange=t.exchange,
            industry=profile.get('industry') or t.industry,
            sector=profile.get('sector') or t.sector,
            country=profile.get('country') or t.country,
            currency=profile.get('currency') or t.currency,
            source=t.source,
            type_from=t.type_from,
            is_actively_trading=bool(is_active) if is_active is not None else None,
        )
        ticker.update(updated_ticker)  # stamps updated_at

        ticker.update_invalid(t.id, invalid_reason)  # clears it if the retry now checks out fine
        if invalid_reason:
            marked_invalid += 1
        else:
            updated += 1

    log.record_status(
        f"Ticker profile refresh: {updated} updated, {marked_invalid} marked invalid, "
        f"out of {len(tickers)} checked."
    )
    return len(tickers), updated, marked_invalid


def _elect_master(
    candidate_ids: list[int],
    tickers_by_id: dict[int, Ticker],
    market_caps: dict[int, float],
) -> int:
    """First-election rule: the company's primary listing (home-market listing, else a US
    listing — see modules.ticker.company), else the highest-cap listing, then lowest id."""
    members = [tickers_by_id[tid] for tid in candidate_ids]
    return company.primary_listing(members, None, market_caps).id


def _sync_group(
    member_ids: list[int],
    tickers_by_id: dict[int, Ticker],
    market_caps: dict[int, float],
) -> list[tuple[int, int]]:
    """
    Given a candidate group (all sharing a CIK, or all sharing a normalized name), returns
    the (ticker_id, master_ticker_id) assignments still needed. If any member already has an
    established master, every other member is (re)pointed at that same frozen master — a
    master is never re-elected once set, regardless of how market caps move afterward. Only
    a group with no existing master calls _elect_master.
    """
    existing_masters = {
        tickers_by_id[tid].master_ticker_id
        for tid in member_ids
        if tickers_by_id[tid].master_ticker_id is not None
    }
    master_id = min(existing_masters) if existing_masters else _elect_master(member_ids, tickers_by_id, market_caps)

    return [
        (tid, master_id)
        for tid in member_ids
        if tid != master_id and tickers_by_id[tid].master_ticker_id != master_id
    ]


def _name_key(name: str | None) -> str:
    return (name or '').strip().lower()


def _still_matches(sibling: Ticker, master_ticker: Ticker) -> bool:
    """A link is still valid if both tickers have a CIK and it's the same, or (when either one
    lacks a CIK) if their normalized names still match — the same two criteria
    sync_master_tickers() uses to form a group in the first place."""
    if sibling.cik and master_ticker.cik:
        return sibling.cik == master_ticker.cik
    return bool(_name_key(sibling.name)) and _name_key(sibling.name) == _name_key(master_ticker.name)


def unlink_stale_master_tickers() -> int:
    """
    Unlinks any sibling whose CIK no longer matches its master's, or (for name-based links)
    whose name no longer matches — e.g. after a profile refresh corrected a CIK or company
    name. Run before sync_master_tickers() so an unlinked ticker can be regrouped correctly in
    the same run. Masters themselves are never re-elected; only sibling links are removed.
    Returns the number of tickers unlinked.
    """
    siblings = ticker.fetch_linked_siblings()
    if not siblings:
        return 0
    master_ids = sorted({s.master_ticker_id for s in siblings})
    masters_by_id = {t.id: t for t in ticker.fetch_by_ids(master_ids)}

    stale: list[int] = []
    for s in siblings:
        m = masters_by_id.get(s.master_ticker_id)
        if m is None or not _still_matches(s, m):
            stale.append(s.id)
            log.record_notice(
                f"Unlinked ticker_id={s.id} ({s.symbol}, {s.name}) from master ticker_id={s.master_ticker_id}"
                + (f" ({m.symbol}, {m.name})" if m else "") + ": CIK/name no longer match."
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
        while master_of.get(node) is not None and node not in path:
            path.append(node)
            node = master_of[node]
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
    of their own (the one exception to "masters are frozen"). Other no-CIK names group among
    themselves under the usual elect-and-freeze rule (_sync_group).
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
        root = t.master_ticker_id if t.master_ticker_id in by_id else t.id
        members_of.setdefault(root, []).append(t)

    grouped_ids = [t.id for ms in members_of.values() if len(ms) > 1 for t in ms]
    caps = ticker_value.fetch_latest_market_caps(grouped_ids) if grouped_ids else {}

    cap_updates: list[tuple[int, float | None]] = []
    region_updates: list[tuple[int, str]] = []
    for root, members in members_of.items():
        region = company.region(members, root)
        region_updates.extend((t.id, region) for t in members)
        if len(members) > 1:
            cap_updates.append((root, company.company_market_cap(members, root, caps)))

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
    flattened before and after grouping."""
    unlinked = unlink_stale_master_tickers()
    repair_master_chains()
    masters_updated = sync_master_tickers()
    masters_updated += repair_master_chains()
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

    return "\n".join(lines)
