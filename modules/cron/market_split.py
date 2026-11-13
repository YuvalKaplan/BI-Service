"""
The market split: each fund bucket's share of the market, measured on our own benchmarks - the
float-adjusted total (whole company cap x float factor, capped at 1; unknown counts in full) of

- region: the US and International full-universe (large blend) benchmarks - US vs International;
- style: the US blend's value / growth benchmarks - growth vs value (US only: International has
  no style split).

Recalculated quarterly, on the first generation day of each calendar quarter (the first run in a
quarter with no split stored for it), from that day's benchmark snapshots, and stored in
benchmark_split. Funds with "split": "market" (region and / or style) take their buckets' targets
from the split in force on their rebalancing date; a change of target is never forced - the
bucket above its new target stops buying, freed places go to the bucket below it.

Float factors are today's (ticker.float_factor keeps no history).
"""
import log
from dataclasses import dataclass, field
from datetime import date
from psycopg.errors import Error
from modules.core.db import db_pool_instance
from modules.object import benchmark, benchmark_split
from modules.object.benchmark_split import BenchmarkSplit, REGION, STYLE
from modules.ticker import company, index_funds

REGION_PARTS = (company.US, company.INTERNATIONAL)
STYLE_PARTS = ('growth', 'value')


@dataclass
class MarketSplitStats:
    snapshot_date: date | None = None
    # split_type -> the parts in force after the run, and whether this run recalculated them
    splits: dict[str, list[BenchmarkSplit]] = field(default_factory=dict)
    recalculated: list[str] = field(default_factory=list)
    notices: list[str] = field(default_factory=list)


def quarter(d: date) -> tuple[int, int]:
    return d.year, (d.month - 1) // 3 + 1


def _float_totals(benchmark_ids: list[int], holding_date: date) -> dict[int, tuple[int, float]]:
    """{benchmark_id: (companies, float-adjusted total)} of the benchmarks' holdings on the date,
    each company's float factor read through its current master."""
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT bh.benchmark_id, count(*),
                           sum(bh.market_cap * least(coalesce(m.float_factor, 1), 1))
                    FROM benchmark_holding bh
                    JOIN ticker t ON t.id = bh.ticker_id
                    JOIN ticker m ON m.id = coalesce(t.master_ticker_id, t.id)
                    WHERE bh.benchmark_id = ANY(%s) AND bh.holding_date = %s
                    GROUP BY bh.benchmark_id;
                    """,
                    (benchmark_ids, holding_date),
                )
                return {r[0]: (int(r[1]), float(r[2])) for r in cur.fetchall()}
    except Error as e:
        raise Exception(f"Error summing the benchmarks' float-adjusted caps on {holding_date}: {e}")


def _blend_ids() -> dict[str, int]:
    """{region: id} of each region's enabled large-cap blend / core (full-universe) benchmark."""
    out: dict[str, int] = {}
    for b in benchmark.fetch_all():
        if b.cap_type == 'large' and b.style_type in index_funds.FULL_UNIVERSE_STYLES and not b.disabled:
            out.setdefault(b.region, b.id)
    return out


def _parts(split_type: str, ids: dict[str, int], snapshot_date: date) -> list[BenchmarkSplit] | None:
    """The split's parts from the benchmarks' snapshots on the date; None when one is missing."""
    totals = _float_totals(list(ids.values()), snapshot_date)
    if any(i not in totals for i in ids.values()):
        return None
    whole = sum(totals[i][1] for i in ids.values())
    return [BenchmarkSplit(as_of_date=snapshot_date, split_type=split_type, part=part, benchmark_id=i,
                           companies=totals[i][0], float_cap=totals[i][1], weight=totals[i][1] / whole)
            for part, i in ids.items()]


def compute(snapshot_date: date) -> tuple[dict[str, list[BenchmarkSplit]], list[str]]:
    """({split_type: parts}, notices) from the benchmark snapshots on the date - a split whose
    benchmarks are missing or have no snapshot that day is left out, with a notice."""
    out: dict[str, list[BenchmarkSplit]] = {}
    notices: list[str] = []
    blends = _blend_ids()
    if all(r in blends for r in REGION_PARTS):
        region_ids = {r: blends[r] for r in REGION_PARTS}
        parts = _parts(REGION, region_ids, snapshot_date)
        if parts:
            out[REGION] = parts
        else:
            notices.append(f"region split: a blend benchmark has no snapshot on {snapshot_date}")
    else:
        notices.append("region split: no enabled US and International blend benchmarks")
    styles = {b.style_type: b.id for b in benchmark.fetch_style_split(blends[company.US])} if company.US in blends else {}
    if all(s in styles for s in STYLE_PARTS):
        parts = _parts(STYLE, {s: styles[s] for s in STYLE_PARTS}, snapshot_date)
        if parts:
            out[STYLE] = parts
        else:
            notices.append(f"style split: a US style benchmark has no snapshot on {snapshot_date}")
    else:
        notices.append("style split: no enabled US value and growth benchmarks")
    return out, notices


def _latest_snapshot_date(up_to: date) -> date | None:
    blends = _blend_ids()
    if company.US not in blends:
        return None
    try:
        with db_pool_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT max(holding_date) FROM benchmark_holding WHERE benchmark_id = %s AND holding_date <= %s;',
                            (blends[company.US], up_to))
                return cur.fetchone()[0]
    except Error as e:
        raise Exception(f"Error fetching the latest US blend snapshot date: {e}")


def refresh(snapshot_date: date | None = None, force: bool = False) -> MarketSplitStats:
    """
    Recalculates each split from the benchmark snapshots on snapshot_date (default: the latest US
    blend snapshot up to today) unless one is already stored in that date's quarter (force: always).
    The cron calls it on every generation day, after the benchmarks: only the first in a quarter
    recalculates.
    """
    snapshot_date = snapshot_date or _latest_snapshot_date(date.today())
    stats = MarketSplitStats(snapshot_date=snapshot_date)
    if snapshot_date is None:
        stats.notices.append("no benchmark snapshot to measure the market split on")
        log.record_status("Market split: " + stats.notices[-1])
        return stats

    due = []
    for split_type in (REGION, STYLE):
        current = benchmark_split.fetch_in_force(split_type, snapshot_date)
        if force or not current or current[0].as_of_date > snapshot_date or quarter(current[0].as_of_date) != quarter(snapshot_date):
            due.append(split_type)
        else:
            stats.splits[split_type] = current

    if due:
        computed, notices = compute(snapshot_date)
        stats.notices += notices
        for split_type in due:
            if split_type in computed:
                benchmark_split.replace_for_date(snapshot_date, split_type, computed[split_type])
                stats.splits[split_type] = computed[split_type]
                stats.recalculated.append(split_type)
            elif (current := benchmark_split.fetch_in_force(split_type, snapshot_date)):
                stats.splits[split_type] = current   # keeps the last one
    log.record_status("Market split: " + "; ".join(_line(t, p, t in stats.recalculated) for t, p in stats.splits.items()))
    return stats


def _line(split_type: str, parts: list[BenchmarkSplit], recalculated: bool) -> str:
    order = REGION_PARTS if split_type == REGION else STYLE_PARTS
    by_part = {p.part: p for p in parts}
    shares = " / ".join(f"{by_part[k].weight:.1%} {k}" for k in order if k in by_part)
    when = "recalculated" if recalculated else f"in force since {parts[0].as_of_date}"
    return f"{split_type} {shares} ({when}, benchmarks of {parts[0].as_of_date})"


def summary(stats: MarketSplitStats) -> str:
    """The cron email's section: each split in force, and whether this run recalculated it."""
    lines = ["Market split (quarterly, float-adjusted benchmark totals)"]
    for split_type, parts in stats.splits.items():
        lines.append(f"- {_line(split_type, parts, split_type in stats.recalculated)}")
        lines += [f"  - {p.part}: ${p.float_cap / 1e12:.2f}T, {p.companies:,} companies" for p in parts]
    lines += [f"- Notice: {n}" for n in stats.notices]
    return "\n".join(lines)
