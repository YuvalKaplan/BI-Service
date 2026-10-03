"""
The list of actively managed ETFs (sec_active_etf), read from the funds' own Form N-CEN filings
on EDGAR.

Every registered fund files N-CEN once a year, within 75 days of its fiscal year end, and states
its type there (Item C.3: exchange-traded fund, index fund, fund of funds, ...). An ETF that
isn't an index fund is actively managed. run() reads EDGAR's form index for the last
WINDOW_QUARTERS quarters and every N-CEN / N-CEN/A filing in it not read yet, oldest first:
- a filing's active ETFs are upserted, so each series keeps its latest filing (carried forward
  until the next one, a year later);
- a series it reports as an index fund (or no longer an ETF) is removed;
- the series it lists as terminated - all of its series when it's the registrant's last
  filing - are marked terminated.
Rows only ever move to a newer filing, so filings can be read in any order and read again.

EDGAR rather than the SEC's quarterly N-CEN data sets: those appear weeks after quarter end
and can miss filings (2025 Q4 lacks 97 of the quarter's 598, e.g. Amplify ETF Trust's).
"""
import log
import os
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, timedelta
from modules.object import batch_run, sec_active_etf, sec_ncen_filing
from modules.object.batch_run import BatchRun
from modules.object.sec_active_etf import SecActiveEtf
from modules.object.sec_ncen_filing import SecNcenFiling
from modules.sec import edgar
from modules.sec.edgar import IndexEntry

NCEN_FORMS = ('N-CEN', 'N-CEN/A')
WINDOW_QUARTERS = 5         # a filing year plus slack for late filers, so the first run builds the whole list
CURRENT_WINDOW_DAYS = 455   # no filing in ~15 months (liquidated, no termination listed yet): off the current list
MAX_FAILED_SHARE = 0.1      # more failed filings than this in a run fails the run
MIN_FAILED_TO_RAISE = 5     # ... but a handful never does
PROGRESS_EVERY = 250        # filings between progress lines
DOWNLOAD_WORKERS = 4        # parallel filing downloads, all under edgar's 5-requests-per-second throttle
FAILED_XML_DIR = os.path.join('.output', 'downloads', 'ncen')

# Item C.3 fund-type labels as they appear in the XML (fundTypes/fundType text, or the fundType
# attribute of e.g. indexFundInfo). Multiple / inverse funds are labelled "Inverse of a
# benchmark" (the 2x long ones included), "Multiple of a benchmark", ...
LABEL_ETF = 'Exchange-Traded Fund'
LABEL_ETMF = 'Exchange-Traded Managed Fund'
LABEL_INDEX = 'Index Fund'
LABEL_FUND_OF_FUNDS = 'Fund of Funds'


@dataclass
class NcenFund:
    series_id: str
    fund_name: str
    ticker: str | None
    adviser_name: str | None
    is_etf: bool
    is_etmf: bool
    is_index: bool
    is_fund_of_funds: bool
    is_multiple_inverse: bool
    net_assets: float | None

    @property
    def is_active_etf(self) -> bool:
        return self.is_etf and not self.is_index


@dataclass
class NcenFiling:
    entry: IndexEntry
    registrant_name: str | None
    registrant_cik: str | None
    report_period: date | None
    is_last_filing: bool
    funds: list[NcenFund]
    terminated: list[tuple[str, date | None]]  # (series id, termination month)
    reported_series: list[str]                 # the series the filing reports on (its header)

    def terminations(self) -> list[tuple[str, date]]:
        """(series id, termination date) to mark: the listed terminated series, and on the
        registrant's last filing every series it reports on, as of the report period."""
        fallback = self.report_period or self.entry.filed
        out = {series_id: when or fallback for series_id, when in self.terminated}
        if self.is_last_filing:
            for series_id in self.reported_series + [f.series_id for f in self.funds]:
                out.setdefault(series_id, fallback)
        return list(out.items())


@dataclass
class NcenRunStats:
    window: str                                         # e.g. 2025q3-2026q3
    indexed: int = 0                                    # N-CEN filings in EDGAR's index for the window
    to_read: int = 0                                    # not read yet (all of them with reload)
    read: int = 0
    failed: list[str] = field(default_factory=list)     # "accession (registrant): error"
    etf_series: int = 0                                 # ETF series in the filings read
    added: int = 0
    updated: int = 0
    removed: int = 0                                    # a newer filing says index fund / not an ETF
    terminated: int = 0
    current: int = 0                                    # the current list after the run
    tracked: int = 0                                    # ... of which in provider_etf (equity funds)


def _local(tag: str) -> str:
    return tag.rsplit('}', 1)[-1]


def _text(el: ET.Element | None, tag: str) -> str | None:
    if el is None:
        return None
    value = (el.findtext(f'{{*}}{tag}') or '').strip()
    return value or None


def _float(value: str | None) -> float | None:
    try:
        return float(value) if value else None
    except ValueError:
        return None


def _iso_date(value: str | None) -> date | None:
    try:
        return date.fromisoformat(value) if value else None
    except ValueError:
        return None


def _month_date(value: str | None) -> date | None:
    """'01/2025' (the terminated-series format) -> 2025-01-01; ISO dates are accepted too."""
    if not value:
        return None
    try:
        month, year = value.split('/')
        return date(int(year), int(month), 1)
    except ValueError:
        return _iso_date(value)


def _fund_types(question: ET.Element) -> set[str]:
    labels: set[str] = set()
    fund_types = question.find('{*}fundTypes')
    if fund_types is None:
        return labels
    for el in fund_types.iter():
        if _local(el.tag) == 'fundType' and (el.text or '').strip():
            labels.add(el.text.strip())
        if el.get('fundType'):
            labels.add(el.get('fundType').strip())
    return labels


def parse_filing(xml: bytes, entry: IndexEntry) -> NcenFiling:
    """The registrant, report period, funds (with their Item C.3 types, listed ticker and
    adviser) and terminated series of one N-CEN XML document."""
    root = ET.fromstring(xml)
    general = root.find('.//{*}generalInfo')
    registrant = root.find('.//{*}registrantInfo')

    # Part E lists each ETF's exchange listings; its ticker is the listed one.
    listed: dict[str, str | None] = {}
    for etf in root.iterfind('.//{*}exchangeSeriesInfo/{*}exchangeTradedFund'):
        series_id = _text(etf, 'etfSeriesId')
        if series_id:
            exchange = etf.find('.//{*}securityExchange')
            listed[series_id] = (exchange.get('fundsTickerSymbol') or None) if exchange is not None else None

    funds = []
    for question in root.iterfind('.//{*}managementInvestmentQuestion'):
        series_id = _text(question, 'mgmtInvSeriesId')
        if not series_id:  # closed-end funds and other registrants without series
            continue
        labels = _fund_types(question)
        class_ticker = next((s.get('sharesOutstandingTickerSymbol') for s in question.iterfind('.//{*}sharesOutstanding')
                             if s.get('sharesOutstandingTickerSymbol')), None)
        funds.append(NcenFund(
            series_id=series_id,
            fund_name=_text(question, 'mgmtInvFundName') or series_id,
            ticker=(listed.get(series_id) or class_ticker or '').strip().upper() or None,
            adviser_name=_text(question.find('{*}investmentAdvisers/{*}investmentAdviser'), 'investmentAdviserName'),
            is_etf=series_id in listed or LABEL_ETF in labels or LABEL_ETMF in labels,
            is_etmf=LABEL_ETMF in labels,
            is_index=LABEL_INDEX in labels,
            is_fund_of_funds=LABEL_FUND_OF_FUNDS in labels,
            is_multiple_inverse=any('multiple' in l.lower() or 'inverse' in l.lower() for l in labels),
            net_assets=_float(_text(question, 'mnthlyAvgNetAssets')),
        ))

    return NcenFiling(
        entry=entry,
        registrant_name=_text(registrant, 'registrantFullName') or entry.company,
        registrant_cik=_text(registrant, 'registrantCik') or entry.cik,
        report_period=_iso_date(general.get('reportEndingPeriod')) if general is not None else None,
        is_last_filing=_text(registrant, 'isRegistrantLastFiling') == 'Y',
        funds=funds,
        terminated=[(t.get('seriesId'), _month_date(t.get('terminationDate')))
                    for t in root.iterfind('.//{*}terminatedSeriesInfo') if t.get('seriesId')],
        reported_series=[s.text.strip() for s in root.iterfind('.//{*}headerData//{*}rptSeriesClassInfo/{*}seriesId')
                         if s.text and s.text.strip()],
    )


def _window(today: date) -> list[tuple[int, int]]:
    """(year, quarter) of the last WINDOW_QUARTERS quarters, oldest first, the current one last."""
    index = today.year * 4 + (today.month - 1) // 3
    return [((i // 4), (i % 4) + 1) for i in range(index - WINDOW_QUARTERS + 1, index + 1)]


def _save_failed_xml(entry: IndexEntry, xml: bytes) -> None:
    try:
        os.makedirs(FAILED_XML_DIR, exist_ok=True)
        with open(os.path.join(FAILED_XML_DIR, f'{entry.accession}.xml'), 'wb') as f:
            f.write(xml)
    except OSError:
        pass


def _failed(entry: IndexEntry, stats: NcenRunStats, error: Exception) -> None:
    message = str(error)[:500]
    stats.failed.append(f"{entry.accession} ({entry.company}): {message}")
    sec_ncen_filing.upsert(SecNcenFiling(
        accession_number=entry.accession, cik=entry.cik, registrant_name=entry.company,
        form_type=entry.form_type, filing_date=entry.filed, error=message,
    ))


def _download(entry: IndexEntry) -> tuple[IndexEntry, bytes | None, Exception | None]:
    try:
        return entry, edgar.filing_xml(entry.cik, entry.accession), None
    except Exception as e:
        return entry, None, e


def _apply(entry: IndexEntry, xml: bytes | None, error: Exception | None, stats: NcenRunStats) -> None:
    """Parses one downloaded filing and applies it. A download or parse failure is stored with
    the filing (read again next run); a database failure raises."""
    if error is not None or xml is None:
        _failed(entry, stats, error or Exception('empty download'))
        return
    try:
        filing = parse_filing(xml, entry)
    except Exception as e:
        _save_failed_xml(entry, xml)
        _failed(entry, stats, e)
        return

    active = [f for f in filing.funds if f.is_active_etf]
    items = [SecActiveEtf(
        series_id=f.series_id, fund_name=f.fund_name, ticker=f.ticker,
        registrant_name=filing.registrant_name, registrant_cik=filing.registrant_cik, adviser_name=f.adviser_name,
        is_etmf=f.is_etmf, is_fund_of_funds=f.is_fund_of_funds, is_multiple_inverse=f.is_multiple_inverse,
        net_assets=f.net_assets, report_period=filing.report_period,
        filing_date=entry.filed, accession_number=entry.accession,
    ) for f in active]
    added, updated, removed, terminated = sec_active_etf.apply_filing(
        items,
        removed_series=[f.series_id for f in filing.funds if not f.is_active_etf],
        terminated=filing.terminations(),
        report_period=filing.report_period,
        filing_date=entry.filed,
    )
    sec_ncen_filing.upsert(SecNcenFiling(
        accession_number=entry.accession, cik=entry.cik, registrant_name=filing.registrant_name,
        form_type=entry.form_type, filing_date=entry.filed, report_period=filing.report_period,
        funds=len(filing.funds), etfs=sum(f.is_etf for f in filing.funds), active_etfs=len(active),
    ))
    stats.read += 1
    stats.etf_series += sum(f.is_etf for f in filing.funds)
    stats.added += added
    stats.updated += updated
    stats.removed += removed
    stats.terminated += terminated


def filed_since(as_of: date | None = None) -> date:
    """The earliest filing date a fund on the current list can have."""
    return (as_of or date.today()) - timedelta(days=CURRENT_WINDOW_DAYS)


def summary(stats: NcenRunStats) -> str:
    """The cron email's section: a title, then one short bullet per fact."""
    lines = [
        f"SEC active ETFs (N-CEN, {stats.window})",
        f"- {stats.read:,} new filing(s) read of {stats.indexed:,} indexed",
    ]
    if stats.failed:
        lines.append(f"- {len(stats.failed)} failed:")
        lines += [f"  - {f}" for f in stats.failed[:10]]
        if len(stats.failed) > 10:
            lines.append(f"  - ... and {len(stats.failed) - 10} more (sec_ncen_filing.error)")
    lines += [
        f"- {stats.added:,} ETF(s) added",
        f"- {stats.updated:,} updated",
        f"- {stats.removed:,} removed (index fund / not an ETF by a newer filing)",
        f"- {stats.terminated:,} terminated",
        f"- Current list: {stats.current:,} active ETFs",
        f"- {stats.tracked:,} of them equity funds in provider_etf",
    ]
    return "\n".join(lines)


def run(reload: bool = False, as_of: date | None = None) -> NcenRunStats:
    """
    Reads the N-CEN / N-CEN/A filings of the last WINDOW_QUARTERS quarters not read yet (all of
    them with reload) into sec_active_etf, oldest first. Filings that fail to download or parse
    are stored with their error and read again next run.

    Raises - so the cron emails the failure - when EDGAR's form index can't be read or more than
    MAX_FAILED_SHARE of the filings fail.
    """
    today = as_of or date.today()
    quarters = _window(today)
    batch_run_id = batch_run.insert(BatchRun(process='ncen_active_etfs', activation='auto'))
    stats = NcenRunStats(window=f"{quarters[0][0]}q{quarters[0][1]}-{quarters[-1][0]}q{quarters[-1][1]}")
    log.record_status(f"Starting SEC N-CEN active ETFs batch job ID {batch_run_id} for {stats.window}{' (reload)' if reload else ''}")
    try:
        entries: dict[str, IndexEntry] = {}
        for year, quarter in quarters:
            for entry in edgar.form_index(year, quarter, NCEN_FORMS):
                entries[entry.accession] = entry
        stats.indexed = len(entries)

        processed = set() if reload else sec_ncen_filing.fetch_processed_accessions()
        to_read = sorted((e for a, e in entries.items() if a not in processed), key=lambda e: (e.filed, e.accession))
        stats.to_read = len(to_read)
        log.record_status(f"{stats.indexed} N-CEN filings indexed for {stats.window}, {stats.to_read} to read.")

        # Downloads run in parallel (a filing's download time, not the throttle, is the limit on
        # one thread); filings are applied one at a time, in filing-date order.
        pool = ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS)
        try:
            for n, (entry, xml, error) in enumerate(pool.map(_download, to_read), 1):
                _apply(entry, xml, error, stats)
                if n % PROGRESS_EVERY == 0:
                    print(f"[ncen] {n}/{stats.to_read} filings read ({len(stats.failed)} failed)")
        finally:
            pool.shutdown(wait=True, cancel_futures=True)  # a database failure doesn't wait for the rest

        stats.current, stats.tracked = sec_active_etf.count_current(filed_since(today))
        log.record_status(summary(stats))
        if len(stats.failed) > max(MIN_FAILED_TO_RAISE, MAX_FAILED_SHARE * stats.to_read):
            raise Exception(f"{len(stats.failed)} of {stats.to_read} N-CEN filings failed - first: {stats.failed[0]}")

        batch_run.update_completed_at(batch_run_id)
        log.record_status("SEC N-CEN active ETFs completed.\n")
        return stats

    except Exception as e:
        log.record_error(f"Error in ncen: {e}")
        raise
