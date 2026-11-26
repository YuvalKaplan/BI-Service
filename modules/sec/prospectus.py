"""
Non-transparent ETFs, from their prospectuses (sec_nontransparent_filing).

A non-transparent ("semi-transparent": ActiveShares, proxy portfolio, tracking basket) ETF
doesn't publish its holdings daily - only quarterly, a month or two late. FMP dates its latest
file with every refresh, so its holdings look current while they are months old: identical share
counts day after day, then nearly every line changes the day after the quarterly report
(American Century FDG / FLV / ACLC, T. Rowe Price TCHP / TEQI, Oct 2026). The selection's
'transparent' rule keeps them out.

The SEC's orders that let these funds operate require a legend on their prospectus cover,
config.Prospectus.LEGEND ("This ETF is different from traditional ETFs. Traditional ETFs tell the
public each day ..."). run() finds it with EDGAR's full-text search in the single-fund prospectus
filings (config.Prospectus.FORMS: 497K summary prospectuses, 497 supplements) of the last
config.Prospectus.WINDOW_DAYS, and maps each to its series by the filing's header. A filing
covering several series is left out: a trust-wide filing (485BPOS, a supplement for the whole
trust) carries the legend for its non-transparent series only. Prospectuses are renewed yearly,
so a fund that becomes fully transparent drops off within the window.

Not Form N-CEN's Item C.7.k (reliance on Rule 6c-11, the ETF rule that requires daily holdings):
checked on the filings behind the 246 active ETFs (Nov 2026), it missed 5 of the 15 non-
transparent ones (T. Rowe Price ticks it for TCHP / TDVG / TEQI / TGRW, John Hancock for JHAC) and
wasn't ticked by 27 transparent ones (CAMX, LSVD, Brown Advisory, Touchstone, Parnassus, ...).

Run by hand monthly with the SEC list (scripts/current_sec_active_etfs.py) - the SEC blocks the
hosted cron's IP - before the profiles and the selection.
"""
import log
from dataclasses import dataclass, field
from datetime import date, timedelta
from modules import config
from modules.object import provider_etf, sec_nontransparent_filing
from modules.object.sec_nontransparent_filing import SecNontransparentFiling
from modules.sec import edgar


@dataclass
class ProspectusRunStats:
    window: str                                         # e.g. 2025-08-28 to 2026-11-26
    found: int = 0                                      # filings with the legend in the window
    new: int = 0                                        # ... not stored yet, headers read
    stored: int = 0                                     # ... single-fund ones stored
    multi_series: int = 0                               # ... covering several series (left out)
    failed: list[str] = field(default_factory=list)     # "accession: error"
    series: int = 0                                     # non-transparent series in the window
    tracked: list[str] = field(default_factory=list)    # ... provider ETFs among them (tickers)


def window_start(as_of: date | None = None) -> date:
    """The earliest filing date of a legend that still counts."""
    return (as_of or date.today()) - timedelta(days=config.Prospectus.WINDOW_DAYS)


def non_transparent_series(as_of: date | None = None) -> set[str]:
    """The series with a legend filed within the window (the selection's 'transparent' rule)."""
    return sec_nontransparent_filing.fetch_series(window_start(as_of))


def summary(stats: ProspectusRunStats) -> str:
    """The run's report: a title, then one short bullet per fact."""
    lines = [
        f"Non-transparent ETFs (prospectus legend, {stats.window})",
        f"- {stats.found} filing(s) with the legend, {stats.new} new",
        f"- {stats.stored} single-fund filing(s) stored, {stats.multi_series} covering several series left out",
    ]
    if stats.failed:
        lines.append(f"- {len(stats.failed)} failed (read again next run):")
        lines += [f"  - {f}" for f in stats.failed[:10]]
    lines.append(f"- {stats.series} non-transparent series, {len(stats.tracked)} of them provider ETFs"
                 + (f": {', '.join(sorted(stats.tracked))}" if stats.tracked else ""))
    return "\n".join(lines)


def run(as_of: date | None = None) -> ProspectusRunStats:
    """
    Stores the single-fund prospectus filings of the window that carry the legend and aren't
    stored yet. A filing whose header can't be read is read again next run.

    Raises when the search fails or more than config.Prospectus.MAX_FAILED_SHARE of the new filings fail.
    """
    today = as_of or date.today()
    start = window_start(today)
    stats = ProspectusRunStats(window=f"{start} to {today}")
    log.record_status(f"Starting the non-transparent ETF prospectus search for {stats.window}")
    try:
        hits = edgar.full_text_search(config.Prospectus.LEGEND, config.Prospectus.FORMS, start, today)
        stats.found = len(hits)
        stored = sec_nontransparent_filing.fetch_accessions()
        items = []
        for hit in hits:
            if hit.accession in stored:
                continue
            stats.new += 1
            try:
                series = edgar.filing_series(hit.cik, hit.accession)
            except Exception as e:
                stats.failed.append(f"{hit.accession}: {str(e)[:300]}")
                continue
            if len(series) != 1:
                stats.multi_series += 1
                continue
            items.append(SecNontransparentFiling(accession_number=hit.accession, series_id=series[0], cik=hit.cik,
                                                 form_type=hit.form_type, filing_date=hit.filed))
        sec_nontransparent_filing.upsert_many(items)
        stats.stored = len(items)

        flagged = non_transparent_series(today)
        stats.series = len(flagged)
        stats.tracked = [e.ticker or e.name or str(e.id) for e in provider_etf.fetch_all() if e.sec_series_id in flagged]
        log.record_status(summary(stats))
        if len(stats.failed) > max(config.Prospectus.MIN_FAILED_TO_RAISE, config.Prospectus.MAX_FAILED_SHARE * stats.new):
            raise Exception(f"{len(stats.failed)} of {stats.new} prospectus filings failed - first: {stats.failed[0]}")
        return stats

    except Exception as e:
        log.record_error(f"Error in prospectus: {e}")
        raise
