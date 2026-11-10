"""
Replays every active fund from scratch over the generation days since inception_date, with the
current code and data – after a methodology fix or a data correction, or to give a new fund a
history – so the live cron carries on from the replayed holdings.

1. Every stored weekly screen a generation day uses (the latest on or before the day before it,
   as live) is rebuilt: the screened companies (company_builder – today's masters, regions and
   validated values) and every enabled benchmark (benchmark_generator – that date's breakpoints,
   today's float factors and styles), so each week's benchmarks include any added since (the US
   value / growth split).
2. The active funds' fund_holding / fund_holding_change / fund_analysis are erased, and every best
   idea from the first generation day's look-back on.
3. On every generation day, best ideas are generated for all active ETFs and every active fund is
   recalculated when its recalc_frequency_days have passed – the cron's own steps.

Unlike scripts/sim_fund.py (one fund, development only, benchmarks backfilled from today's large
caps) it replays all active funds together and runs on production too, after asking for
confirmation. Run it on a day the cron doesn't generate, or before that day's run: the replay ends
the day before it's run.

Report: .output/funds_replay_<first day>_to_<last day>.txt

Usage:
    python scripts/funds_replay.py --dev
    python scripts/funds_replay.py --prod
"""
import atexit
import os
from datetime import date, timedelta
import log
from modules.core.db import ENVIRONMENT
from modules.object.exit import cleanup
from modules.object import best_idea, fund, fund_analysis, fund_holding, fund_holding_change, screener_listing
from modules.cron import benchmark_generator, best_ideas_generator, company_builder, funds_update, schedule
from modules.calc import model_fund

atexit.register(cleanup)

INCEPTION_DATE = date(2026, 10, 1)  # <-- edit before each run: the first generation day on or after it starts the replay
REBUILD_BENCHMARKS = True           # <-- False keeps the stored screened companies and benchmarks as they are


def screen_dates(days: list[date]) -> list[date]:
    """The stored screen each generation day uses: the latest on or before the day before it (live,
    the generation day's screen is dated at the previous close)."""
    dates = {screener_listing.fetch_latest_date(up_to=d - timedelta(days=1)) for d in days}
    return sorted(d for d in dates if d is not None)


def confirm(funds: list, days: list[date], screens: list[date]) -> bool:
    print(f"\nEnvironment: {ENVIRONMENT}")
    print(f"Generation days: {', '.join(str(d) for d in days)}")
    if REBUILD_BENCHMARKS:
        print(f"Screens rebuilt (companies + benchmarks): {', '.join(str(d) for d in screens)}")
    print(f"Best ideas deleted and regenerated from: {days[0] - timedelta(days=best_ideas_generator.LOOK_BACK_WINDOW)}")
    print("Active funds erased and replayed:")
    for f in funds:
        print(f"  {f.id}: {f.name}")
    if ENVIRONMENT != 'production':
        return True
    return input("\nThis rewrites production history. Type 'replay production' to continue: ").strip() == 'replay production'


def main() -> None:
    end = date.today() - timedelta(days=1)
    days = schedule.generation_days_between(INCEPTION_DATE, end)
    if not days:
        raise RuntimeError(f"No generation day between {INCEPTION_DATE} and {end}.")
    funds = fund.fetch_all()  # active funds
    if not funds:
        raise RuntimeError("No active funds.")
    screens = screen_dates(days)
    if not confirm(funds, days, screens):
        print("Cancelled - nothing changed.")
        return

    report: list[str] = [
        f"Funds replay ({ENVIRONMENT}): {days[0]} to {days[-1]}, {len(days)} generation day(s)",
        "",
    ]

    # 1. Companies and benchmarks, per stored screen.
    if REBUILD_BENCHMARKS:
        report.append("Benchmarks rebuilt")
        for screen_date in screens:
            company_builder.run(screen_date)
            stats = benchmark_generator.run(screen_date)
            for name, holdings, _total, coverage, cutoff in stats.benchmarks:
                report.append(f"- {screen_date} {name}: {holdings} companies, cutoff ${cutoff / 1e9:,.1f}B ({coverage:.0%})")
        report.append("")

    # 2. Erase the funds' history and the best ideas the replay regenerates.
    for f in funds:
        fund_holding.delete_all_for_fund(f.id)
        fund_holding_change.delete_all_for_fund(f.id)
        fund_analysis.delete_all_for_fund(f.id)
    deleted = best_idea.delete_from(days[0] - timedelta(days=best_ideas_generator.LOOK_BACK_WINDOW))
    log.record_status(f"[funds_replay] Erased {len(funds)} funds' history and {deleted} best ideas.")

    # 3. The generation days: best ideas for every active ETF, then every active fund.
    look_back = max(model_fund.getStrategyFromJson(f.strategy).recalc_frequency_days for f in funds)
    sections: dict[int, list[str]] = {f.id: [] for f in funds}
    for day in days:
        log.record_status(f"[funds_replay] Generation day {day}")
        best_ideas_generator.run(as_of_date=day)
        all_best_ideas_df, mc_map = funds_update.build_shared_context(day, look_back)
        for f in funds:
            results = funds_update.activate_fund(f.id, day, all_best_ideas_df, mc_map)
            if results is not None:
                header = f"Recalculation on {day}"
                sections[f.id].append(f"{header}\n{'-' * len(header)}\n" + model_fund.results_to_string(results, include_header=False))

    for f in funds:
        report += [f"{'=' * 70}", f"{f.name} (fund_id={f.id})", f"{'=' * 70}", ""]
        report += ["\n\n".join(sections[f.id]) or "No recalculation.", ""]

    os.makedirs(".output", exist_ok=True)
    path = os.path.join(".output", f"funds_replay_{days[0]}_to_{days[-1]}.txt")
    with open(path, "w", encoding="utf-8") as out:
        out.write("\n".join(report))
    print(f"\nReplayed {len(funds)} fund(s) over {len(days)} generation day(s). Report: {path}")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        log.record_error(f"[funds_replay] Failed: {e}")
        print(f"Error in funds replay: {e}")
