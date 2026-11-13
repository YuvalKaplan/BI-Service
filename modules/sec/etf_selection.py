"""
Which provider ETFs feed best ideas: the selection rules, applied to every provider_etf row
(the SEC active ETF list's actively managed equity funds, modules/sec/etf_profile.py).

The rules keep out the funds whose holdings wouldn't express a manager's stock picks within a
broad large-cap market: derivative strategies, single-country and emerging-market funds, sector
themes and funds mostly outside the large-cap universe our benchmarks measure. Each is a test
with a round level, not one fitted to the pool of the day. An ETF is active when it is still on
the current SEC list, its profile still finds it an equity fund, and:
  1. its holdings have never shown a derivative contract (contracts): an option - FMP lists
     them as "AAPL US 10/16/26 C340", "SPY 11/17/2026 762.29 C", "SPY CALL USD 12/04/2026",
     "JPMORGAN CHASE + CO NOV26 355 CALL", "NOV26 AVGO C @ 390" or an OCC code
     ("USAR  261016C00020000") - or an equity-linked note ("WELLS FARGO ELN 10/27", "Wells Fargo &
     Co 147.40% 12/15/2026" - a level of ELN_MIN_LEVEL or more before a date, never a bill's
     coupon). Covered-call and other option-income funds hold the stocks of an ordinary fund, but
     their weights follow the option book. Once seen (provider_etf.contracts_found - in a holdings
     download or a profile's holdings) the fund is out for good;
  2. its region is US or International (REGIONS) - a Global fund has no benchmark and no fund
     region to feed;
  3. its cap size is large or all (CAP_TYPES);
  4. it holds MIN_STOCK_HOLDINGS to MAX_STOCK_HOLDINGS[region] stocks (lines matched to the index
     funds' stocks) - international managers spread over more names;
  5. no sector is more than MAX_SECTOR_EXCESS above its reference weight: the highest weight that
     sector has in our most specific benchmarks - each region's blend, or its value and growth
     benchmarks where it's split (sector_reference). A growth fund may hold as much technology
     as the growth benchmark, a value fund as much finance as the value one, plus the margin; a
     sector theme exceeds both. Measured on the fund's latest stored holdings, by our tickers'
     sectors;
  6. its top country is at most MAX_TOP_COUNTRY_WEIGHT of the fund - a single-country fund - unless
     it's a US fund whose top country is the US (our companies' domicile, a US-region company
     counting as US);
  7. an International fund has at most MAX_EMERGING_WEIGHT in emerging markets (stocks the
     emerging index fund holds - index_funds.EMERGING_FUNDS): the funds are developed
     international;
  8. at least MIN_COVERAGE_ENTRY of its stock weight is in companies of its region's blend
     benchmark (coverage) - a fund mostly in mid and small caps, or outside the benchmarks' universe,
     has best ideas the benchmark can't measure. An active fund stays while at MIN_COVERAGE_STAY,
     so it doesn't drop in and out around one level; a fund not yet active, or never measured
     while active, needs the entry level. Measured on the fund's latest stored holdings, every
     company through its current master;
  9. its latest stored holdings (provider_etf_holding) are at most MAX_HOLDINGS_AGE_DAYS old. FMP
     refreshes most funds' holdings weekly (dated Sunday), some on other weekdays; two weeks
     covers the longest fund look-back (recalc_frequency_days, at most 14), so every active ETF
     can feed every fund. Best ideas and the valuation pass look back as far.
A fund that moved to another trust is on the list twice under one ticker until its old series
drops off (BRIF, TGLR): only the one with the latest filing can be active - the other fails
'duplicate' (they'd download the same holdings and count twice in the funds).
Otherwise it's inactive - except a fund that passes everything but has no holdings stored yet,
which stays pending until its first download (the holdings rules - sector, coverage - wait for
it). The holdings download (modules/cron/etf_downloader.py) fetches the active and pending funds,
and those failing only the rules its holdings decide (HOLDINGS_RULES: freshness, sector,
coverage - download_targets), so a fund comes back as soon as FMP refreshes it or its holdings
change.

run() follows every profile run (the monthly SEC list update, by hand) and holdings download (the
daily run days). It also sets each ETF's benchmark_id - its region's enabled large-cap blend /
core benchmark, which the market best ideas compare it with (with its value / growth split, if any) - and stores the measures
behind rules 5 and 8 (sector_excess, excess_sector, benchmark_coverage).
"""
import log
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from modules.object import benchmark, provider_etf, provider_etf_holding, sec_active_etf
from modules.object.provider_etf import ProviderEtf, ACTIVE, INACTIVE, PENDING
from modules.sec import ncen
from modules.ticker import index_funds

REGIONS = ('US', 'International')
CAP_TYPES = ('large', 'all')
MIN_STOCK_HOLDINGS = 20
MAX_STOCK_HOLDINGS = {'US': 200, 'International': 250}
MAX_SECTOR_EXCESS = 0.20        # a sector's weight over its highest weight in our benchmarks (sector_reference)
MAX_TOP_COUNTRY_WEIGHT = 0.35
MAX_EMERGING_WEIGHT = 0.35
MIN_COVERAGE_ENTRY = 0.70       # stock weight in the region's blend benchmark companies to become active...
MIN_COVERAGE_STAY = 0.65        # ... and to stay active
MAX_HOLDINGS_AGE_DAYS = 14
ELN_MIN_LEVEL = 20.0            # a percentage before a date at or above this is an equity-linked note's level, not a coupon

EQUITY = 'equity'   # the profile's strategy for an actively managed equity fund (etf_profile.classify)
RULES = ('sec', 'duplicate', 'equity', 'contracts', 'region', 'cap', 'holdings', 'sector', 'country', 'emerging',
         'coverage', 'fresh')
HOLDINGS_RULES = {'fresh', 'sector', 'coverage'}    # decided by the stored holdings: checked again on each download

_MONTH = r'(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)'
_DATE = r'(?:\b\d{1,2}/\d{1,2}/\d{2,4}\b|\b' + _MONTH + r'\d{2}\b)'
_OCC_CODE = re.compile(r'\b[A-Z][A-Z0-9.]{0,5}\s*\d{6}[CP]\d{8}\b', re.IGNORECASE)
_OPTION_NAME = re.compile(
    _DATE + r'.*(?:\s[CP]\s?\d+(?:\.\d+)?$|\s\d+(?:\.\d+)?\s[CP]$|\b(?:CALL|PUT)S?\b)'
    r'|\b(?:CALL|PUT)S?\b.*' + _DATE + r'|\s[CP] @ \d', re.IGNORECASE)
_ELN_NAME = re.compile(r'\beln\b', re.IGNORECASE)
_ELN_LEVEL = re.compile(r'(\d+(?:\.\d+)?)%\s+\d{1,2}/\d{1,2}/\d{2,4}\b')


def is_contract_line(symbol: str | None, name: str | None) -> bool:
    """An FMP holdings line that is an option contract or an equity-linked note (rule 1). Treasury
    bills, money-market funds, currency forwards ("AUD999999 10/02/2026") and rights carry dates
    too, but no strike, call / put or note level."""
    s, n = (symbol or '').strip(), (name or '').strip()
    if _OCC_CODE.search(s) or _OCC_CODE.search(n) or _OPTION_NAME.search(n) or _ELN_NAME.search(n):
        return True
    level = _ELN_LEVEL.search(n)
    return bool(level and float(level.group(1)) >= ELN_MIN_LEVEL)


def has_contract_lines(lines: list[dict]) -> bool:
    """Whether any of FMP's holdings lines (etf/holdings rows) is a contract."""
    return any(is_contract_line(r.get('asset'), r.get('name')) for r in lines)


@dataclass
class HoldingsMeasures:
    """Rules 5 and 8 on an ETF's latest stored holdings."""
    sector_excess: float | None = None        # its sector's weight most above that sector's reference weight
    excess_sector: str | None = None
    benchmark_coverage: float | None = None   # stock weight in its region's blend benchmark companies


@dataclass
class Evaluated:
    etf: ProviderEtf
    failed: list[str]
    latest: date | None     # its latest stored holdings date
    status: str
    benchmark_id: int | None
    measures: HoldingsMeasures = field(default_factory=HoldingsMeasures)


@dataclass
class SelectionStats:
    as_of: date
    by_status: Counter = field(default_factory=Counter)
    activated: list[str] = field(default_factory=list)
    deactivated: list[str] = field(default_factory=list)         # "TICKER (rules)"
    changed: int = 0


def coverage_level(etf: ProviderEtf) -> float:
    """The coverage an ETF needs: the stay level when it's active and was measured while active
    (benchmark_coverage stored), else the entry level."""
    return MIN_COVERAGE_STAY if etf.status == ACTIVE and etf.benchmark_coverage is not None else MIN_COVERAGE_ENTRY


def failed_rules(etf: ProviderEtf, current_series: set[str], latest: date | None, as_of: date,
                 superseded: bool = False, measures: HoldingsMeasures | None = None) -> list[str]:
    """The rules the ETF fails (RULES order); an unknown value fails, except that the holdings
    measures wait for the first holdings download. superseded: another ETF with its ticker has a
    later filing."""
    m = measures or HoldingsMeasures()
    failed = []
    if not etf.sec_series_id or etf.sec_series_id not in current_series:
        failed.append('sec')
    if superseded:
        failed.append('duplicate')
    if etf.strategy != EQUITY:
        failed.append('equity')
    if etf.contracts_found is not None:
        failed.append('contracts')
    if etf.region not in REGIONS:
        failed.append('region')
    if etf.cap_type not in CAP_TYPES:
        failed.append('cap')
    max_holdings = MAX_STOCK_HOLDINGS.get(etf.region or '')
    if etf.stock_holdings is None or max_holdings is None or not MIN_STOCK_HOLDINGS <= etf.stock_holdings <= max_holdings:
        failed.append('holdings')
    if latest is not None and (m.sector_excess is None or m.sector_excess > MAX_SECTOR_EXCESS):
        failed.append('sector')
    single_country = etf.top_country_weight is None or etf.top_country_weight > MAX_TOP_COUNTRY_WEIGHT
    if single_country and not (etf.region == 'US' and etf.top_country == 'US'):
        failed.append('country')
    if etf.region == 'International' and (etf.emerging_weight is None or etf.emerging_weight > MAX_EMERGING_WEIGHT):
        failed.append('emerging')
    if latest is not None and (m.benchmark_coverage is None or m.benchmark_coverage < coverage_level(etf)):
        failed.append('coverage')
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


def sector_reference(weights: dict[int, dict[int, tuple[float, str | None]]], blend_ids: dict[str, int]) -> dict[str, float]:
    """{sector: its highest weight in our most specific benchmarks}: each region's blend, or its
    value / growth benchmarks where the region is split. weights: {benchmark_id: {company:
    (weight, sector)}} of the latest snapshots."""
    references = set(weights)
    for blend_id in blend_ids.values():
        if any(b.id in weights for b in benchmark.fetch_style_split(blend_id)):
            references.discard(blend_id)
    out: dict[str, float] = {}
    for bid in references:
        total = sum(w for w, _ in weights[bid].values()) or 1.0
        by_sector: dict[str, float] = defaultdict(float)
        for w, sector in weights[bid].values():
            if sector:
                by_sector[sector] += w / total
        for sector, w in by_sector.items():
            out[sector] = max(out.get(sector, 0.0), w)
    return out


def holdings_measures(as_of: date) -> dict[int, HoldingsMeasures]:
    """{provider_etf_id: rules 5 and 8 on its latest stored holdings}: its resolved stock lines
    (positive weight) by company (current master) and sector (ours), against the benchmarks'
    latest snapshots on or before as_of."""
    blend_ids = _benchmark_ids()
    bench = benchmark.fetch_latest_company_weights(as_of)
    reference = sector_reference(bench, blend_ids)
    blend_companies = {region: set(bench.get(bid, {})) for region, bid in blend_ids.items()}
    by_etf: dict[int, list[tuple[int, float, str | None]]] = defaultdict(list)
    regions: dict[int, str | None] = {}
    for etf_id, region, company_id, weight, sector in provider_etf_holding.fetch_latest_company_weights():
        by_etf[etf_id].append((company_id, weight, sector))
        regions[etf_id] = region
    out: dict[int, HoldingsMeasures] = {}
    for etf_id, lines in by_etf.items():
        total = sum(w for _, w, _ in lines)
        m = HoldingsMeasures()
        out[etf_id] = m
        if total <= 0:
            continue
        sectors: dict[str, float] = defaultdict(float)
        for _, w, sector in lines:
            if sector:
                sectors[sector] += w / total
        if sectors:
            m.excess_sector = max(sectors, key=lambda s: sectors[s] - reference.get(s, 0.0))
            m.sector_excess = sectors[m.excess_sector] - reference.get(m.excess_sector, 0.0)
        companies = blend_companies.get(regions[etf_id] or '')
        if companies:
            m.benchmark_coverage = sum(w for c, w, _ in lines if c in companies) / total
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
    measures = holdings_measures(today)
    etfs = provider_etf.fetch_all()
    newest = _newest_by_ticker(etfs, filed)
    out = []
    for etf in etfs:
        latest = latest_dates.get(etf.id) if etf.id is not None else None
        m = measures.get(etf.id, HoldingsMeasures()) if etf.id is not None else HoldingsMeasures()
        superseded = bool(etf.ticker) and newest.get((etf.ticker or '').upper()) != etf.id
        failed = failed_rules(etf, set(filed), latest, today, superseded, m)
        out.append(Evaluated(etf=etf, failed=failed, latest=latest, status=status_for(failed, latest),
                             benchmark_id=benchmarks.get(etf.region or ''), measures=m))
    return out


def download_targets(as_of: date | None = None) -> list[Evaluated]:
    """The ETFs whose holdings are downloaded: those failing only rules their holdings decide."""
    return [e for e in evaluate(as_of) if set(e.failed) <= HOLDINGS_RULES and e.etf.ticker]


def _same(a: float | None, b: float | None) -> bool:
    return a == b if a is None or b is None else abs(a - b) < 1e-9


def _changed(e: Evaluated) -> bool:
    etf, m = e.etf, e.measures
    return (e.status != etf.status or e.benchmark_id != etf.benchmark_id or m.excess_sector != etf.excess_sector
            or not _same(m.sector_excess, etf.sector_excess) or not _same(m.benchmark_coverage, etf.benchmark_coverage))


def run(as_of: date | None = None) -> SelectionStats:
    """Applies the rules to every provider ETF and writes the statuses, benchmarks and holdings
    measures that changed."""
    today = as_of or date.today()
    stats = SelectionStats(as_of=today)
    changes: list[provider_etf.SelectionUpdate] = []
    for e in evaluate(today):
        stats.by_status[e.status] += 1
        if e.etf.id is None or not _changed(e):
            continue
        m = e.measures
        changes.append(provider_etf.SelectionUpdate(
            id=e.etf.id, status=e.status, benchmark_id=e.benchmark_id, sector_excess=m.sector_excess,
            excess_sector=m.excess_sector, benchmark_coverage=m.benchmark_coverage))
        if e.status == e.etf.status:
            continue
        stats.changed += 1
        label = e.etf.ticker or e.etf.name or str(e.etf.id)
        if e.status == ACTIVE:
            stats.activated.append(label)
        elif e.etf.status == ACTIVE:
            stats.deactivated.append(f"{label} ({', '.join(e.failed) or e.status})")
    provider_etf.update_selection(changes)
    log.record_status(summary(stats))
    return stats


def summary(stats: SelectionStats) -> str:
    """The cron email's section: a title, then one short bullet per fact."""
    lines = [
        f"ETF selection {stats.as_of}",
        f"- {stats.by_status[ACTIVE]} active",
        f"- {stats.by_status[PENDING]} pending",
        f"- {stats.by_status[INACTIVE]} inactive",
        f"- {stats.changed} changed status",
    ]
    if stats.activated:
        lines.append(f"- Activated ({len(stats.activated)}): {', '.join(sorted(stats.activated))}")
    if stats.deactivated:
        lines.append(f"- Deactivated ({len(stats.deactivated)}):")
        lines += [f"  - {d}" for d in sorted(stats.deactivated)]
    return "\n".join(lines)
