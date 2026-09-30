"""
Which provider ETFs feed best ideas: the selection rules, applied to every provider_etf row
(the SEC active ETF list's actively managed equity funds, modules/sec/etf_profile.py).

An ETF is active when it is still on the current SEC list, its profile still finds it an equity
fund, and:
  1. its region is US or International (REGIONS) - a Global fund has no benchmark and no fund
     region to feed;
  2. its cap size is large or all (CAP_TYPES);
  3. it holds MIN_STOCK_HOLDINGS to MAX_STOCK_HOLDINGS stocks (lines matched to the index funds'
     stocks);
  4. its top sector is at most MAX_TOP_SECTOR_WEIGHT of the fund (FMP's sector weights, else our
     tickers' sectors - etf_profile.profile_fund);
  5. its latest stored holdings (provider_etf_holding) are at most MAX_HOLDINGS_AGE_DAYS old. FMP
     refreshes most funds' holdings weekly (dated Sunday), some on other weekdays, so this allows
     a week and a few days' slack; best ideas and the valuation pass look back as far.
A fund that moved to another trust is on the list twice under one ticker until its old series
drops off (BRIF, TGLR): only the one with the latest filing can be active - the other fails
'duplicate' (they'd download the same holdings and count twice in the funds).
Otherwise it's inactive - except a fund that passes everything but has no holdings stored yet,
which stays pending until its first download. The holdings download (modules/cron/etf_downloader.py)
fetches the active and pending funds, and those failing only rule 5 (download_targets), so a fund
comes back as soon as FMP refreshes it.

run() follows every profile run (Sunday) and holdings download (Tuesday to Saturday). It also
sets each ETF's benchmark_id: its region's enabled large-cap blend / core benchmark, which the
full_universe best ideas compare it with.
"""
import log
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, timedelta
from modules.object import benchmark, provider_etf, provider_etf_holding, sec_active_etf
from modules.object.provider_etf import ProviderEtf, ACTIVE, INACTIVE, PENDING
from modules.sec import ncen
from modules.ticker import index_funds

REGIONS = ('US', 'International')
CAP_TYPES = ('large', 'all')
MIN_STOCK_HOLDINGS = 20
MAX_STOCK_HOLDINGS = 200
MAX_TOP_SECTOR_WEIGHT = 0.40
MAX_HOLDINGS_AGE_DAYS = 10

EQUITY = 'equity'   # the profile's strategy for an actively managed equity fund (etf_profile.classify)
RULES = ('sec', 'duplicate', 'equity', 'region', 'cap', 'holdings', 'sector', 'fresh')


@dataclass
class Evaluated:
    etf: ProviderEtf
    failed: list[str]
    latest: date | None     # its latest stored holdings date
    status: str
    benchmark_id: int | None


@dataclass
class SelectionStats:
    as_of: date
    by_status: Counter = field(default_factory=Counter)
    failing: Counter = field(default_factory=Counter)             # rule -> inactive ETFs failing it
    activated: list[str] = field(default_factory=list)
    deactivated: list[str] = field(default_factory=list)         # "TICKER (rules)"
    changed: int = 0


def failed_rules(etf: ProviderEtf, current_series: set[str], latest: date | None, as_of: date,
                 superseded: bool = False) -> list[str]:
    """The rules the ETF fails (RULES order); an unknown value fails. superseded: another ETF
    with its ticker has a later filing."""
    failed = []
    if not etf.sec_series_id or etf.sec_series_id not in current_series:
        failed.append('sec')
    if superseded:
        failed.append('duplicate')
    if etf.strategy != EQUITY:
        failed.append('equity')
    if etf.region not in REGIONS:
        failed.append('region')
    if etf.cap_type not in CAP_TYPES:
        failed.append('cap')
    if etf.stock_holdings is None or not MIN_STOCK_HOLDINGS <= etf.stock_holdings <= MAX_STOCK_HOLDINGS:
        failed.append('holdings')
    if etf.top_sector_weight is None or etf.top_sector_weight > MAX_TOP_SECTOR_WEIGHT:
        failed.append('sector')
    if latest is None or latest < as_of - timedelta(days=MAX_HOLDINGS_AGE_DAYS):
        failed.append('fresh')
    return failed


def status_for(failed: list[str], latest: date | None) -> str:
    if not failed:
        return ACTIVE
    if failed == ['fresh'] and latest is None:
        return PENDING      # passes everything it can before its first holdings download
    return INACTIVE


def _benchmark_ids() -> dict[str, int]:
    """{region: id} of each region's enabled large-cap blend / core benchmark."""
    out: dict[str, int] = {}
    for b in benchmark.fetch_all():
        if b.cap_type == 'large' and b.style_type in index_funds.FULL_UNIVERSE_STYLES:
            out.setdefault(b.region, b.id)
    return out


def _newest_by_ticker(etfs: list[ProviderEtf], filed: dict[str, date]) -> dict[str, int | None]:
    """{ticker: id of the ETF with that ticker and the latest filing (current series first)}."""
    ranked = sorted(etfs, key=lambda e: (e.sec_series_id in filed, filed.get(e.sec_series_id or '') or date.min, e.id or 0))
    return {(e.ticker or '').upper(): e.id for e in ranked if e.ticker}   # the last - the newest - wins


def evaluate(as_of: date | None = None) -> list[Evaluated]:
    """Every provider ETF with the rules it fails and the status and benchmark they give it."""
    today = as_of or date.today()
    filed = {e.series_id: e.filing_date for e in sec_active_etf.fetch_current(ncen.filed_since(today))}
    latest_dates = provider_etf_holding.fetch_latest_dates()
    benchmarks = _benchmark_ids()
    etfs = provider_etf.fetch_all()
    newest = _newest_by_ticker(etfs, filed)
    out = []
    for etf in etfs:
        latest = latest_dates.get(etf.id) if etf.id is not None else None
        superseded = bool(etf.ticker) and newest.get((etf.ticker or '').upper()) != etf.id
        failed = failed_rules(etf, set(filed), latest, today, superseded)
        out.append(Evaluated(etf=etf, failed=failed, latest=latest, status=status_for(failed, latest),
                             benchmark_id=benchmarks.get(etf.region or '')))
    return out


def download_targets(as_of: date | None = None) -> list[Evaluated]:
    """The ETFs whose holdings are downloaded: those passing every rule but freshness."""
    return [e for e in evaluate(as_of) if set(e.failed) <= {'fresh'} and e.etf.ticker]


def run(as_of: date | None = None) -> SelectionStats:
    """Applies the rules to every provider ETF and writes the statuses and benchmarks that changed."""
    today = as_of or date.today()
    stats = SelectionStats(as_of=today)
    changes: list[tuple[int, str, int | None]] = []
    for e in evaluate(today):
        stats.by_status[e.status] += 1
        if e.status == INACTIVE:
            stats.failing.update(e.failed)
        if e.etf.id is None or (e.status == e.etf.status and e.benchmark_id == e.etf.benchmark_id):
            continue
        changes.append((e.etf.id, e.status, e.benchmark_id))
        label = e.etf.ticker or e.etf.name or str(e.etf.id)
        if e.status == ACTIVE and e.etf.status != ACTIVE:
            stats.activated.append(label)
        elif e.status != ACTIVE and e.etf.status == ACTIVE:
            stats.deactivated.append(f"{label} ({', '.join(e.failed) or e.status})")
    provider_etf.update_selection(changes)
    stats.changed = len(changes)
    log.record_status(summary(stats))
    return stats


def summary(stats: SelectionStats) -> str:
    """A few lines for the cron email."""
    failing = ", ".join(f"{r} {stats.failing[r]}" for r in RULES if stats.failing[r])
    lines = [
        f"ETF selection {stats.as_of}: {stats.by_status[ACTIVE]} active, {stats.by_status[PENDING]} pending, "
        f"{stats.by_status[INACTIVE]} inactive ({stats.changed} changed) - inactive failing: {failing or 'none'}",
    ]
    if stats.activated:
        lines.append(f"  activated ({len(stats.activated)}): {', '.join(sorted(stats.activated))}")
    if stats.deactivated:
        lines.append(f"  deactivated ({len(stats.deactivated)}): {', '.join(sorted(stats.deactivated))}")
    return "\n".join(lines)
