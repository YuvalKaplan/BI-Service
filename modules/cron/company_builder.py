import log
from dataclasses import dataclass, field
from datetime import date, timedelta
from modules.core import api_stocks
from modules.cron import screener
from modules.cron.screener import SCREENER_EXCHANGES, describe, row_symbol
from modules.object import batch_run, ticker, ticker_value, screener_listing, screener_company
from modules.object.screener_listing import ScreenerListing, LINE_HOME, LINE_FOREIGN
from modules.object.ticker import Ticker
from modules.object.screener_company import ScreenerCompany
from modules.ticker import company, identity, valuation
from modules.ticker import util as tu

RECENT_CAP_DAYS = 21  # caps the duplicate guard compares on a common date
VALUE_WINDOW_DAYS = 5  # a listing's value may be this many days older than the screen date (a market holiday)


@dataclass
class CompanyBuildStats:
    screen_date: date
    home_lines: int = 0
    no_value: list[str] = field(default_factory=list)            # left out: no market cap stored for the screen date
    non_equity: list[str] = field(default_factory=list)          # left out: a note / preferred line (company.is_non_equity_line)
    foreign_duplicates: list[str] = field(default_factory=list)  # skipped: foreign line of a company already in
    foreign_currency: list[str] = field(default_factory=list)    # skipped: foreign line quoted in another currency
    foreign_admitted: list[str] = field(default_factory=list)    # foreign line admitted (company not found elsewhere)
    duplicate_companies: list[str] = field(default_factory=list) # dropped by the company-level duplicate guard
    us_companies: int = 0
    intl_companies: int = 0


def summary(stats: CompanyBuildStats) -> str:
    """One line for the cron email."""
    return (
        f"Company builder {stats.screen_date}: {stats.us_companies} US and {stats.intl_companies} International companies — "
        f"{len(stats.no_value)} listings without a validated value (or invalid) and {len(stats.non_equity)} note/preferred "
        f"lines left out; skipped "
        f"{len(stats.foreign_duplicates)} foreign duplicate and {len(stats.foreign_currency)} other-currency lines; "
        f"admitted {len(stats.foreign_admitted)} foreign lines; dropped {len(stats.duplicate_companies)} duplicate companies"
    )


def _listing_caps(
    listings: list[ScreenerListing],
    screen_date: date,
    require_value: bool,
    stats: CompanyBuildStats,
) -> dict[int, tuple[str, float]]:
    """
    {ticker_id: (exchange, market cap in USD)} for the registered listings: the latest validated
    value stored on screen_date or up to VALUE_WINDOW_DAYS before it — a market closed on the
    screen date (all of JPX on a Japanese holiday) keeps its last close. Without one (withheld
    in its grace period all week) the listing is left out when require_value (live: a snapshot
    needs a genuinely validated value); the sim passes False and falls back to the latest stored
    value — it re-fetches each listing's whole history anyway, so one bad data point mustn't
    throw a company out of every historical generation day. A listing flagged invalid is always left out,
    and so is a note or preferred line by its current data (company.is_non_equity_line: FMP reports
    no equity float for it, or its name is cut after a coupon) — the screener classified it by the
    screen's name alone, and a company screened only through such a line (Algonquin via its
    notes AQNB, whose FMP cap is the note's price x the company's shares) isn't a large-cap company.
    """
    ids = [l.ticker_id for l in listings if l.ticker_id]
    tickers_by_id = {t.id: t for t in ticker.fetch_by_ids(ids)}
    invalid = {tid: t.invalid for tid, t in tickers_by_id.items() if t.invalid}
    recent = ticker_value.fetch_market_caps_between(ids, screen_date - timedelta(days=VALUE_WINDOW_DAYS), screen_date)
    latest = {} if require_value else ticker_value.fetch_latest_values(ids)
    out: dict[int, tuple[str, float]] = {}
    for l in listings:
        if not l.ticker_id:
            continue
        if l.ticker_id in invalid:
            stats.no_value.append(f"{describe(l)} — invalid: {invalid[l.ticker_id][:80]}")
            continue
        t = tickers_by_id.get(l.ticker_id)
        if t and company.is_non_equity_line(t.symbol, t.name, t.exchange, t.free_float):
            stats.non_equity.append(f"{describe(l)} — {t.name}" + (" (no equity float)" if t.free_float == 0 else ""))
            continue
        values = recent.get(l.ticker_id)
        cap = values[max(values)] if values else None
        if cap is None and l.ticker_id in latest:
            cap = latest[l.ticker_id][1]
        if cap is None:
            stats.no_value.append(describe(l))
            continue
        out[l.ticker_id] = (l.exchange, cap)
    return out


def _company_index(tickers: list[Ticker]) -> tuple[dict[int, Ticker], dict[int, int]]:
    by_id = {t.id: t for t in tickers}
    return by_id, {t.id: company.company_id(t, by_id) for t in tickers}


def _screen_foreign_lines(
    foreign: list[ScreenerListing],
    admitted_ids: set[int],
    stats: CompanyBuildStats,
) -> list[ScreenerListing]:
    """
    Returns the foreign lines (LINE_FOREIGN) to admit: those whose company isn't already in —
    not a listing of an admitted company (after the master sync), no ISIN shared with
    one, no company name (depositary descriptors cut) matching one of the same domicile — and
    whose FMP currency is their exchange's (a line quoted in another currency is a mirror of
    another market's data: Toyota on LSE in JPY). What's left is a company listed only outside
    its domicile: dsm-firmenich (CH) in Amsterdam, Prada (IT) in Hong Kong.
    """
    all_t = ticker.fetch_all()
    by_id, company_of = _company_index(all_t)
    by_pair = {(t.symbol, t.exchange): t for t in all_t}
    admitted = {company_of[tid] for tid in admitted_ids if tid in company_of}
    members: dict[int, list[Ticker]] = {}
    for t in all_t:
        if company_of[t.id] in admitted:
            members.setdefault(company_of[t.id], []).append(t)
    admitted_isins = {t.isin for ms in members.values() for t in ms if t.isin}
    names_by_key: dict[tuple[str, str], list[str]] = {}
    for ms in members.values():
        for t in ms:
            toks = tu.name_tokens(identity.core_name(t.name))
            if toks and t.name:
                names_by_key.setdefault((t.country or '', toks[0]), []).append(identity.core_name(t.name))

    to_add: list[ScreenerListing] = []
    for l in foreign:
        symbol, exchange = row_symbol(l.symbol), l.exchange
        country, core = l.country or '', identity.core_name(l.company_name)
        known = by_pair.get((symbol, exchange))
        toks = tu.name_tokens(core)
        if known and company_of.get(known.id) in admitted:
            master = by_id[company_of[known.id]]
            stats.foreign_duplicates.append(f"{describe(l)} — listing of {master.symbol}:{master.exchange}")
            continue
        if known and known.isin and known.isin in admitted_isins:
            stats.foreign_duplicates.append(f"{describe(l)} — ISIN {known.isin}")
            continue
        match = next((n for n in names_by_key.get((country, toks[0]), []) if tu.names_match(core, n)), None) if toks else None
        if match:
            stats.foreign_duplicates.append(f"{describe(l)} — name matches {match}")
            continue
        currency = known.currency if known and known.currency else None
        if currency is None:
            profile = api_stocks.get_stock_profile(l.symbol)
            currency = profile.get('currency') if isinstance(profile, dict) else None
        if tu.normalize_currency(currency) != tu.currency_for_exchange(exchange):
            stats.foreign_currency.append(f"{describe(l)} — quoted in {currency}")
            continue
        stats.foreign_admitted.append(describe(l))
        to_add.append(l)
    return to_add


def _one_row_per_company(listing_caps: dict[int, tuple[str, float]]) -> list[tuple[int, str, float]]:
    """
    [(company_ticker_id, region, market_cap_usd)], one row per company. FMP reports the whole
    company's market cap on every listing, so listings are never summed — the company's cap is
    its master's company_market_cap (taken from its primary listing by
    master.refresh_company_data), else the listing's own value; its region is the company's.
    """
    tickers_by_id = {t.id: t for t in ticker.fetch_by_ids(list(listing_caps))}
    missing_masters = {
        t.master_ticker_id for t in tickers_by_id.values() if t.master_ticker_id
    } - set(tickers_by_id.keys())
    if missing_masters:
        tickers_by_id.update({t.id: t for t in ticker.fetch_by_ids(list(missing_masters))})

    out: dict[int, tuple[int, str, float]] = {}
    for tid, (exchange, market_cap) in listing_caps.items():
        t = tickers_by_id.get(tid)
        cid = company.company_id(t, tickers_by_id) if t else tid
        if cid in out:
            continue
        c = tickers_by_id.get(cid)
        cap = (c.company_market_cap if c and c.company_market_cap else None) or market_cap
        region = (c.region if c else None) or (company.US if exchange in tu.US_LISTING_EXCHANGES else company.INTERNATIONAL)
        out[cid] = (cid, region, cap)
    return list(out.values())


def drop_duplicate_companies(
    rows: list[tuple[int, str, float]],
    stats: CompanyBuildStats | None = None,
    as_of: date | None = None,
) -> list[tuple[int, str, float]]:
    """
    Last safety net — companies the master sync failed to merge. rows: [(company_id, region,
    market_cap)]. Keeps one row per real company: companies anchored in their home market (or
    domiciled where we screen no exchange) first, then the rest by market (a US listing, another
    country's exchange, OTC), each by cap; each dropped when identity.duplicate_evidence ties it
    to a company already kept — across both regions, so a company can't sit in the US
    benchmark via its ADR and in the International one via its home listing. Caps are compared
    over the RECENT_CAP_DAYS up to as_of (default today).
    """
    if not rows:
        return rows
    groups = ticker.fetch_group_members([cid for cid, _r, _mc in rows])
    ids = [t.id for ms in groups.values() for t in ms]
    as_of = as_of or date.today()
    recent = ticker_value.fetch_market_caps_between(ids, as_of - timedelta(days=RECENT_CAP_DAYS), as_of)
    latest_dates = {tid: max(vals) for tid, vals in recent.items() if vals}
    views = identity.build_views({cid: ms for cid, ms in groups.items() if ms}, recent, latest_dates)

    def anchored(cid: int) -> bool:
        v = views.get(cid)
        return v is None or v.home or not tu.has_screened_home(v.country, SCREENER_EXCHANGES)

    kept: list[tuple[int, str, float]] = []
    index: dict[tuple[str, str], list[int]] = {}
    def order(r: tuple[int, str, float]):
        v = views.get(r[0])
        return (0 if anchored(r[0]) else 1, v.tier if v else 4, -r[2], r[0])

    for row in sorted(rows, key=order):
        cid = row[0]
        v = views.get(cid)
        if v is None:
            kept.append(row)
            continue
        keys = identity.blocking_keys(v) | {('isin', i) for i in v.isins}
        candidates = {k for key in keys for k in index.get(key, [])}
        hit = next(((k, ev) for k in sorted(candidates) if (ev := identity.duplicate_evidence(views[k], v, anchored(cid)))), None)
        if hit:
            k, ev = hit
            msg = f"{v.name} ({v.country}, {row[1]}, ${row[2] / 1e9:,.1f}B) — duplicate of {views[k].name} ({ev})"
            log.record_notice(f"Company duplicate guard: {msg}")
            if stats is not None:
                stats.duplicate_companies.append(msg)
            continue
        kept.append(row)
        for key in keys:
            index.setdefault(key, []).append(cid)
    return kept


def run(screen_date: date | None = None, require_value: bool = True) -> CompanyBuildStats:
    """
    Builds the screened companies (one row per company) from the stored screen (the latest on or before today
    by default) and stores it in screener_company. Must follow the master sync, which links the
    listings the screener registered to their companies:
      1. the home-market lines with a recent market cap as of the screen date (_listing_caps);
      2. the foreign lines that duplicate no company already in (_screen_foreign_lines),
         registered and valued here (screener.register_listings, valuation.store_values);
      3. one row per company, at its company market cap and region (_one_row_per_company);
      4. the duplicate guard across all companies (drop_duplicate_companies).
    No market-cap floor is applied here: each benchmark applies its own cutoff (its market's
    breakpoint at its market_coverage — benchmark_generator.select_holdings).

    Raises — so the cron stops before the generators — when there's no stored screen or no
    company comes out (the stored companies are then left unchanged).
    """
    batch_run_id = batch_run.insert(batch_run.BatchRun(process='company_builder', activation='auto'))
    log.record_status(f"Starting Company Builder batch job ID {batch_run_id}")
    try:
        screen_date = screen_date or screener_listing.fetch_latest_date(up_to=date.today())
        if screen_date is None:
            raise Exception("No stored screen — run the screener first.")
        listings = screener_listing.fetch_for_date(screen_date)
        stats = CompanyBuildStats(screen_date=screen_date)
        home = [l for l in listings if l.line_type == LINE_HOME]
        foreign = [l for l in listings if l.line_type == LINE_FOREIGN]
        stats.home_lines = len(home)

        caps = _listing_caps(home, screen_date, require_value, stats)
        extra = _screen_foreign_lines(foreign, set(caps), stats)
        if extra:
            screener.register_listings(extra)
            valuation.store_values(valuation.targets_for_listings(extra), screen_date)
            caps.update(_listing_caps(extra, screen_date, require_value, stats))
        log.record_status(
            f"Screen {screen_date}: {len(caps)} listings with a value ({len(stats.no_value)} without); foreign lines: "
            f"{len(stats.foreign_duplicates)} duplicates of companies already in, "
            f"{len(stats.foreign_currency)} quoted in another currency, {len(stats.foreign_admitted)} admitted."
        )

        rows = _one_row_per_company(caps)
        kept = drop_duplicate_companies(rows, stats, screen_date)
        if len(kept) < len(rows):
            log.record_status(f"Dropped {len(rows) - len(kept)} duplicate companies (see notices).")
        if not kept:
            raise Exception(f"No companies to store — no market caps could be validated for {screen_date}. Stored companies left unchanged.")

        screener_company.replace_for_date(screen_date, [
            ScreenerCompany(screen_date=screen_date, ticker_id=cid, region=region, market_cap=cap)
            for cid, region, cap in kept
        ])
        stats.us_companies = sum(1 for _cid, region, _cap in kept if region == company.US)
        stats.intl_companies = len(kept) - stats.us_companies
        log.record_status(f"Companies {screen_date}: {stats.us_companies} US, {stats.intl_companies} International.")

        batch_run.update_completed_at(batch_run_id)
        log.record_status("Company Builder completed.\n")
        return stats

    except Exception as e:
        log.record_error(f"Error in company_builder: {e}")
        raise
