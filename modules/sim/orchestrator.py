import os
import re
import log
from datetime import date, timedelta
from modules.core.db import ENVIRONMENT
from modules.object import provider_etf_holding, fund, fund_holding, fund_holding_change
from modules.cron import best_ideas_generator, funds_update
from modules.calc import model_fund

DEFAULT_INCEPTION_DATE = date(2026, 1, 15)


def run(
    fund_id: int,
    inception_date: date = DEFAULT_INCEPTION_DATE,
    weeks: int | None = None,
    show_holdings: bool = False,
) -> None:
    if ENVIRONMENT == 'production':
        raise RuntimeError("Refusing to run fund simulation against production — it erases the target fund's holdings history.")

    f = fund.fetch_by_id(fund_id)
    if f is None:
        raise RuntimeError(f"Fund not found: fund_id={fund_id}")
    strategy = model_fund.getStrategyFromJson(f.strategy)

    available_end_date = provider_etf_holding.fetch_max_holding_date()
    if available_end_date is None:
        raise RuntimeError("No provider_etf_holding data found — nothing to simulate against.")
    if available_end_date < inception_date:
        raise RuntimeError(f"Latest available holding date ({available_end_date}) is before inception_date ({inception_date}).")

    if weeks is not None:
        requested_end_date = inception_date + timedelta(weeks=weeks)
        end_date = min(requested_end_date, available_end_date)
        if requested_end_date > available_end_date:
            log.record_status(
                f"[sim] Requested {weeks} weeks would end {requested_end_date}, "
                f"but data is only available through {available_end_date} — capping there."
            )
    else:
        end_date = available_end_date

    log.record_status(f"[sim] Erasing prior fund_holding/fund_holding_change for fund_id={fund_id}")
    fund_holding.delete_all_for_fund(fund_id)
    fund_holding_change.delete_all_for_fund(fund_id)

    # First recalculation date: the first Tuesday on/after inception_date.
    days_until_tuesday = (1 - inception_date.weekday()) % 7
    first_recalc_date = inception_date + timedelta(days=days_until_tuesday)

    log.record_status(
        f"[sim] Running fund_id={fund_id} simulation from {inception_date} to {end_date}, "
        f"recalculating every {strategy.recalc_frequency_days} days starting {first_recalc_date}"
    )

    weekly_results: list[tuple[date, model_fund.FundChangesResult]] = []

    current = first_recalc_date
    while current <= end_date:
        best_ideas_generator.run(as_of_date=current)
        all_best_ideas_df, mc_map = funds_update.build_shared_context(current)
        results = funds_update.activate_fund(fund_id, current, all_best_ideas_df, mc_map)
        if results is not None:
            weekly_results.append((current, results))
        current += timedelta(days=strategy.recalc_frequency_days)

    log.record_status(f"[sim] Finished fund_id={fund_id} simulation from {inception_date} to {end_date}")

    write_report(fund_id, inception_date, end_date, weekly_results, show_holdings)


def write_report(
    fund_id: int,
    inception_date: date,
    end_date: date,
    weekly_results: list[tuple[date, model_fund.FundChangesResult]],
    show_holdings: bool = False,
) -> None:
    if not weekly_results:
        log.record_status(f"[sim] No weeks simulated for fund_id={fund_id} — skipping txt report.")
        return

    fund = weekly_results[0][1].fund
    strategy = model_fund.getStrategyFromJson(fund.strategy)

    report_header = (
        f"Fund: {fund.name} (fund_id={fund_id})\n"
        f"Simulation period: {inception_date} to {end_date}\n"
        "\n"
        "Strategy:\n"
        f"  Benchmark: {strategy.benchmark}\n"
        f"  Number of holdings: {strategy.holdings}\n"
        f"  Recalculation frequency: {strategy.recalc_frequency_days} days\n"
        "\n" + "=" * 70 + "\n\n"
    )

    sections = []
    for week_date, results in weekly_results:
        header = f"Recalculation on {week_date.isoformat()}"
        sections.append(f"{header}\n{'=' * len(header)}\n" + model_fund.results_to_string(
            results, include_header=False, include_holdings=show_holdings,
        ))
    report_body = report_header + "\n".join(sections)

    safe_name = re.sub(r'[\\/:*?"<>|]', '-', fund.name)
    os.makedirs(".output", exist_ok=True)
    file_name = f"sim_{safe_name}_{fund_id}_{inception_date}_to_{end_date}.txt"
    file_path = os.path.join(".output", file_name)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(report_body)

    log.record_status(f"[sim] Wrote {len(weekly_results)} weekly section(s) to '{file_path}'.")
