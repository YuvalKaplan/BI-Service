"""
The provider ETFs' holdings from FMP (etf/holdings), on the daily run days.

Downloads every ETF the selection rules make a download target (modules/sec/etf_selection.py:
the active and pending ETFs, and those failing only the freshness rule - so a fund comes back as
soon as FMP refreshes it), resolves each line to a ticker, and stores the lines under FMP's
holdings date (their latest updatedAt), replacing any stored for that date. Then the selection
runs again on the new holdings dates.

A line's ticker, first match wins:
  1. one of our tickers by FMP symbol, ISIN or CUSIP (index_funds.matched_ticker_id) - no FMP call;
  2. none for a line that isn't a stock (cash, currencies, money-market funds, derivatives -
     etf_profile.is_stock_line) or has no positive weight (a short or an accrual);
  3. the same line (symbol, ISIN, CUSIP, name) in the ETF's previous holdings: its ticker - this
     covers the stocks FMP lists by name only ("SAMSUNG ELECTRONICS CO"). A line left unresolved
     there is tried again only with retry_unresolved (by default on the generation day -
     modules/cron/schedule.py - before the generators);
  4. resolved from FMP (TickerResolver.resolve_fmp_line: its symbol's profile, else its ISIN, else a
     verified name search), registering a new ticker with its ESG - the ticker maintenance that
     follows (profiles, values, masters, style) takes it from there.
"""
import log
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from modules.core import api_stocks
from modules.cron import schedule
from modules.object import batch_run, provider_etf, provider_etf_holding
from modules.object.batch_run import BatchRun
from modules.object.provider_etf_holding import ProviderEtfHolding
from modules.sec import etf_profile, etf_selection
from modules.sec.etf_selection import Evaluated, SelectionStats
from modules.ticker import index_funds
from modules.ticker.index_funds import Listings
from modules.ticker.resolver import TickerResolver

FETCH_WORKERS = 4               # ETFs fetched in parallel, all under api_stocks' 200-calls-a-minute throttle
MAX_FAILED_SHARE = 0.1
MIN_FAILED_TO_RAISE = 5

LineKey = tuple[str | None, str | None, str | None, str | None]   # symbol, ISIN, CUSIP, name
# How a line got its ticker (or didn't), in the summary's order
MATCHED, CARRIED, RESOLVED, UNRESOLVED, NOT_STOCK = 'matched', 'carried', 'resolved', 'unresolved', 'not_stock'


@dataclass
class DownloadStats:
    as_of: date
    retry_unresolved: bool
    targets: int = 0
    stored: int = 0                                           # ETFs whose holdings were stored
    no_date: list[str] = field(default_factory=list)         # FMP lines without a date: not stored
    failed: list[str] = field(default_factory=list)          # "TICKER: error" - no holdings on FMP included
    lines: int = 0
    outcomes: Counter = field(default_factory=Counter)        # MATCHED ... NOT_STOCK -> lines
    unresolved_weight: dict[str, float] = field(default_factory=dict)   # ETF ticker -> its unresolved stock weight
    selection: SelectionStats | None = None


def _line_key(line: dict) -> LineKey:
    return (line.get('asset') or None, line.get('isin') or None, line.get('securityCusip') or None, line.get('name') or None)


def _previous(etf_id: int, listings: Listings) -> dict[LineKey, int | None]:
    """The ETF's previous lines -> their ticker (None: unresolved), a ticker no longer valid dropped."""
    out: dict[LineKey, int | None] = {}
    for h in provider_etf_holding.fetch_latest_lines(etf_id):
        t = listings.by_id.get(h.ticker_id) if h.ticker_id is not None else None
        if h.ticker_id is None or (t is not None and not t.invalid):
            out[(h.symbol, h.isin, h.cusip, h.name)] = h.ticker_id
    return out


def _resolve(line: dict, previous: dict[LineKey, int | None], listings: Listings, resolver: TickerResolver,
             retry: bool) -> tuple[int | None, str]:
    """The line's ticker and how it got it (see the module docstring)."""
    tid = index_funds.matched_ticker_id(line, listings)
    if tid is not None:
        return tid, MATCHED
    if (line.get('weightPercentage') or 0) <= 0 or not etf_profile.is_stock_line(line, None):
        return None, NOT_STOCK
    key = _line_key(line)
    if key in previous and (previous[key] is not None or not retry):
        return previous[key], CARRIED if previous[key] is not None else UNRESOLVED
    try:
        tid = resolver.resolve_fmp_line(*key)
    except Exception as e:
        log.record_notice(f"Could not resolve holding line {key}: {e}")
        tid = None
    return tid, RESOLVED if tid is not None else UNRESOLVED


def _fetch(target: Evaluated) -> tuple[Evaluated, list[dict], Exception | None]:
    try:
        return target, api_stocks.get_etf_holdings(target.etf.ticker or ''), None
    except Exception as e:
        return target, [], e


def _store(target: Evaluated, rows: list[dict], listings: Listings, resolver: TickerResolver, stats: DownloadStats) -> None:
    etf = target.etf
    label = etf.ticker or str(etf.id)
    day = index_funds.holdings_date(rows)
    if day is None:
        stats.no_date.append(label)
        return
    assert etf.id is not None
    previous = _previous(etf.id, listings)
    lines = []
    unresolved_weight = 0.0
    for r in rows:
        tid, outcome = _resolve(r, previous, listings, resolver, stats.retry_unresolved)
        stats.outcomes[outcome] += 1
        weight = (r['weightPercentage'] / 100) if r.get('weightPercentage') is not None else None
        if outcome == UNRESOLVED and weight:
            unresolved_weight += weight
        lines.append(ProviderEtfHolding(
            id=None, created_at=None, provider_etf_id=etf.id,
            holding_date=datetime.combine(day, datetime.min.time()), ticker_id=tid,
            shares=r.get('sharesNumber'), market_value=r.get('marketValue'), weight=weight,
            symbol=r.get('asset') or None, name=r.get('name') or None, isin=r.get('isin') or None,
            cusip=r.get('securityCusip') or None,
        ))
    provider_etf_holding.replace_holdings(etf.id, datetime.combine(day, datetime.min.time()), lines)
    provider_etf.set_last_downloaded(etf.id, datetime.now(timezone.utc).replace(tzinfo=None))
    stats.stored += 1
    stats.lines += len(lines)
    if unresolved_weight > 0:
        stats.unresolved_weight[label] = unresolved_weight


def summary(stats: DownloadStats) -> str:
    """A few lines for the cron email."""
    lines = [
        f"Provider ETF holdings (FMP) {stats.as_of}: {stats.stored} of {stats.targets} ETF(s) stored, "
        f"{len(stats.no_date)} without a holdings date, {len(stats.failed)} failed"
        f"{' (unresolved lines retried)' if stats.retry_unresolved else ''}",
        f"  {stats.lines} lines: {stats.outcomes[MATCHED]} matched to our tickers, {stats.outcomes[CARRIED]} carried over, "
        f"{stats.outcomes[RESOLVED]} resolved from FMP, {stats.outcomes[UNRESOLVED]} stock line(s) unresolved, "
        f"{stats.outcomes[NOT_STOCK]} not stocks",
    ]
    worst = sorted(stats.unresolved_weight.items(), key=lambda kv: -kv[1])[:5]
    if worst:
        lines.append("  most unresolved stock weight: " + ", ".join(f"{t} {w:.1%}" for t, w in worst))
    if stats.no_date:
        lines.append(f"  without a date: {', '.join(stats.no_date[:10])}")
    lines += [f"  failed: {f}" for f in stats.failed[:10]]
    if stats.selection is not None:
        lines.append(etf_selection.summary(stats.selection))
    return "\n".join(lines)


def run(as_of: date | None = None, retry_unresolved: bool | None = None) -> DownloadStats:
    """
    Downloads, resolves and stores the holdings of every download target, then applies the
    selection rules. Raises - so the cron emails the failure - when more than MAX_FAILED_SHARE of
    the targets fail or come back empty. Lines left unresolved before are tried again with
    retry_unresolved - by default on the generation day (the cron passes its own).
    """
    today = as_of or date.today()
    retry = schedule.is_generation_day(today) if retry_unresolved is None else retry_unresolved
    batch_run_id = batch_run.insert(BatchRun(process='etf_downloader', activation='auto'))
    stats = DownloadStats(as_of=today, retry_unresolved=retry)
    try:
        targets = etf_selection.download_targets(today)
        stats.targets = len(targets)
        log.record_status(f"Running ETF holdings download batch job ID {batch_run_id} - {len(targets)} ETF(s).")
        if targets:
            listings = index_funds.our_listings()
            resolver = TickerResolver(TickerResolver.POPULATE_TICKER)
            pool = ThreadPoolExecutor(max_workers=FETCH_WORKERS)
            try:
                for n, (target, rows, error) in enumerate(pool.map(_fetch, targets), 1):
                    label = target.etf.ticker or str(target.etf.id)
                    if error is not None:
                        stats.failed.append(f"{label}: {str(error)[:200]}")
                    elif not rows:
                        stats.failed.append(f"{label}: no holdings on FMP")
                    else:
                        try:
                            _store(target, rows, listings, resolver, stats)
                        except Exception as e:
                            stats.failed.append(f"{label}: {str(e)[:200]}")
                    if n % 50 == 0:
                        print(f"[etf_downloader] {n}/{len(targets)} ETFs")
            finally:
                pool.shutdown(wait=True, cancel_futures=True)

        if len(stats.failed) > max(MIN_FAILED_TO_RAISE, MAX_FAILED_SHARE * len(targets)):
            log.record_status(summary(stats))
            raise Exception(f"{len(stats.failed)} of {len(targets)} ETF holdings downloads failed - first: {stats.failed[0]}")
        stats.selection = etf_selection.run(today)
        log.record_status(summary(stats))
        batch_run.update_completed_at(batch_run_id)
        return stats

    except Exception as e:
        log.record_error(f"Error in the ETF holdings download: {e}")
        raise
