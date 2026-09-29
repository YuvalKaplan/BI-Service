import log
from dataclasses import dataclass, field
from datetime import date
from modules.object import batch_run, benchmark, ticker, screener_company
from modules.object.benchmark import Benchmark
from modules.object.screener_company import ScreenerCompany
from modules.ticker import index_funds

NO_STYLE_FILTER = ('blend', 'core')


def current_companies(rows: list[ScreenerCompany]) -> list[tuple[int, str, float]]:
    """(company_ticker_id, region, market_cap) per screened company, read through the current
    master — a later master sync may have moved a company's master since the company builder ran."""
    masters = ticker.fetch_master_info_by_ids([r.ticker_id for r in rows])
    out: dict[int, tuple[int, str, float]] = {}
    for r in rows:
        cid = (masters.get(r.ticker_id) or (None, None))[0] or r.ticker_id
        out.setdefault(cid, (cid, r.region, r.market_cap))
    return list(out.values())


def company_styles(company_ids: list[int], benchmarks: list[Benchmark]) -> dict[int, str | None]:
    """{company_id: style_type} — only fetched when a benchmark filters on style."""
    if all(b.style_type in NO_STYLE_FILTER for b in benchmarks):
        return {}
    return {t.id: t.style_type for t in ticker.fetch_by_ids(company_ids)}


def company_float_factors(company_ids: list[int]) -> dict[int, float | None]:
    """{company_id: float_factor} — the float share benchmark membership requires a minimum of."""
    return {t.id: t.float_factor for t in ticker.fetch_by_ids(company_ids)}


def benchmark_cutoffs(benchmarks: list[Benchmark], as_of: date) -> dict[int, float]:
    """{benchmark_id: large-cap cutoff} as of the date: its market's breakpoint at its
    market_coverage (index_funds.cutoff)."""
    return {b.id: index_funds.cutoff(b.region, b.market_coverage, as_of) for b in benchmarks}


def select_holdings(
    companies: list[tuple[int, str, float]],  # (company_ticker_id, region, market_cap)
    benchmarks: list[Benchmark],
    cutoffs: dict[int, float],                # benchmark_id -> large-cap cutoff (benchmark_cutoffs)
    float_factors: dict[int, float | None],   # company_ticker_id -> float_factor
    styles: dict[int, str | None] | None = None,
) -> dict[int, list[tuple[int, float]]]:
    """
    {benchmark_id: [(company_ticker_id, market_cap)]}: the companies each benchmark row
    specifies — its region; a whole company cap at its market's breakpoint for its
    market_coverage and at least index_funds.MIN_FLOAT_FACTOR floating (index_funds.passes_large —
    the S&P / Russell way); and its style_type ('blend'/'core': any style; 'value'/'growth': the
    company's ticker.style_type).
    """
    if styles is None:
        styles = company_styles([cid for cid, _r, _mc in companies], benchmarks)
    return {
        b.id: [
            (cid, mc) for cid, region, mc in companies
            if region == b.region and index_funds.passes_large(mc, float_factors.get(cid), cutoffs[b.id])
            and (b.style_type in NO_STYLE_FILTER or styles.get(cid) == b.style_type)
        ]
        for b in benchmarks
    }


def store_holdings(b: Benchmark, items: list[tuple[int, float]], holding_date: date) -> float:
    """Weights the companies by market cap and stores them as the benchmark's snapshot for
    holding_date (replacing one already there). Returns the total market cap."""
    total = sum(mc for _, mc in items)
    rows = [(ticker_id, mc, mc / total) for ticker_id, mc in items]
    benchmark.insert_holdings(b.id, holding_date, rows)
    log.record_status(f"  {b.name} (id={b.id}) on {holding_date}: {len(rows)} holdings, total market cap ${total/1e12:.2f}T")
    return total


@dataclass
class BenchmarkRunStats:
    holding_date: date
    benchmarks: list[tuple[str, int, float, float, float]] = field(default_factory=list)  # (name, companies, total market cap, coverage, cutoff)


def summary(stats: BenchmarkRunStats) -> str:
    """One line per benchmark for the cron email."""
    return "\n".join(
        f"{name} {stats.holding_date}: {count} companies (cutoff ${cut / 1e9:,.1f}B at {coverage:.0%} of its market), "
        f"total market cap ${total / 1e12:.2f}T"
        for name, count, total, coverage, cut in stats.benchmarks
    )


def run(screen_date: date | None = None) -> BenchmarkRunStats:
    """
    Forms every enabled benchmark in the benchmark table from the stored screened companies (the
    latest on or before today by default; built by modules/cron/company_builder.py) and stores
    each as a market-cap-weighted benchmark_holding snapshot dated at their screen date.

    Raises — so the cron stops before best ideas/funds — when there are no stored companies or any
    benchmark would come out empty (existing snapshots are then left unchanged).
    """
    batch_run_id = batch_run.insert(batch_run.BatchRun(process='benchmark_generator', activation='auto'))
    log.record_status(f"Starting Benchmark Generator batch job ID {batch_run_id}")
    try:
        screen_date = screen_date or screener_company.fetch_latest_date(up_to=date.today())
        if screen_date is None:
            raise Exception("No stored companies — run the company builder first.")
        companies = current_companies(screener_company.fetch_for_date(screen_date))
        benchmarks = benchmark.fetch_all()
        if not benchmarks:
            raise Exception("No enabled benchmarks in the benchmark table.")
        cutoffs = benchmark_cutoffs(benchmarks, screen_date)
        float_factors = company_float_factors([cid for cid, _r, _mc in companies])

        selected = select_holdings(companies, benchmarks, cutoffs, float_factors)
        empty = [b.name for b in benchmarks if not selected[b.id]]
        if empty:
            raise Exception(
                f"Benchmark(s) would be empty on {screen_date}: {', '.join(empty)} "
                f"({len(companies)} screened companies). Existing snapshots left unchanged."
            )

        stats = BenchmarkRunStats(holding_date=screen_date)
        for b in benchmarks:
            total = store_holdings(b, selected[b.id], screen_date)
            stats.benchmarks.append((b.name, len(selected[b.id]), total, b.market_coverage, cutoffs[b.id]))

        batch_run.update_completed_at(batch_run_id)
        log.record_status("Benchmark Generator completed.\n")
        return stats

    except Exception as e:
        log.record_error(f"Error in benchmark_generator: {e}")
        raise
