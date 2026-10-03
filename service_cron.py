import atexit
import log
from datetime import datetime, timezone
from modules.object.exit import cleanup
from modules.core.db import db_pool_instance, ENVIRONMENT
from modules.core import sender
from modules.calc.model_fund import results_to_string
from modules.cron import categorize_downloader, etf_downloader, best_ideas_generator, funds_update, benchmark_generator, screener, company_builder, schedule
from modules.ticker import esg, free_float, index_funds, master, refresh, style, valuation
from modules.sec import etf_profile, ncen

atexit.register(cleanup)


def _duration(seconds: float) -> str:
    minutes = round(seconds / 60)
    return f"{minutes // 60}h {minutes % 60}m" if minutes >= 60 else f"{minutes}m"

if __name__ == '__main__':
    try:
        print("Starting cron service")
        print(f"DB connection pool started with {db_pool_instance.get_max_connections()} connections.")
        
        log.record_status(f"Starting cron service in Environment: {ENVIRONMENT}")

        start_time = datetime.now(timezone.utc)
        run_day = start_time.date()
        weekday = run_day.weekday()
        # The generators' day this week (modules/cron/schedule.py): GENERATION_WEEKDAY, moved a
        # day when the close it would work on is an NYSE holiday.
        generation = schedule.is_generation_day(run_day)
        # The email's sections: a title line, then one short "- " bullet per fact, so it reads on a
        # phone (plain text shows in a proportional font there - aligned columns don't survive).
        sections: list[str] = []

        # A moved generation is reported on its usual day (why nothing is generated) and on the day it runs.
        generation_note = schedule.generation_note(run_day)
        if not (generation_note and (generation or weekday == schedule.GENERATION_WEEKDAY)):
            generation_note = None

        if weekday in schedule.DAILY_RUN_DAYS:
            # Listings in first — the provider ETFs' FMP holdings (stage 2, then the selection rules on
            # their new dates) and the large-cap screener (stage 3), which register their tickers as
            # they read them — then ticker maintenance (stage 4) runs the
            # ticker utilities over every ticker, in order: profiles (so values use current
            # currencies and skip tickers no longer valid) -> values (once per ticker in use; on
            # the generation day also the stored screen's lines) -> share-class consolidation (company
            # caps come from the values) -> style (after grouping, so a new listing isn't classified
            # on its own). The generators only ever use maintained tickers.
            try:
                downloads = etf_downloader.run(retry_unresolved=generation)
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on holdings download with error:\n{e}\n")
                raise e

            sections.append(etf_downloader.summary(downloads))

            try:
                screen = screener.run(store=generation)
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on the FMP large-cap screener with error:\n{e}\n\n")
                raise e

            sections.append(screener.summary(screen))

            try:
                profiles_checked, profiles_updated, profiles_marked_invalid = refresh.refresh_ticker_profiles()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on ticker profile refresh with error:\n{e}\n")
                raise e

            sections.append("\n".join([
                "Ticker profiles",
                f"- {profiles_checked:,} checked",
                f"- {profiles_updated:,} updated",
                f"- {profiles_marked_invalid:,} marked invalid",
            ]))

            try:
                values = valuation.run()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on ticker values with error:\n{e}\n\n")
                raise e

            sections.append(valuation.summary(values))

            try:
                masters_updated, unlinked, caps_updated = master.sync_masters_and_company_data()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on the master ticker sync with error:\n{e}\n\n")
                raise e

            sections.append("\n".join([
                "Ticker master sync",
                f"- {masters_updated:,} link(s)",
                f"- {unlinked:,} unlinked",
                f"- {caps_updated:,} company cap(s) refreshed",
            ]))

            try:
                style.assign_styles()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on style assignment with error:\n{e}\n\n")
                raise e

        if weekday == schedule.WEEKLY_WEEKDAY:  # The weekly day — ticker maintenance, weekly part: style reference data, then ESG; then the SEC active ETF list
            try:
                total_etfs = categorize_downloader.run()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed in categorize ETF download with error:\n{e}\n\n")
                raise e

            try:
                total_esg = esg.refresh_all()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on ESG update with error:\n{e}\n\n")
                raise e

            sections.append("\n".join([
                "Weekly ticker maintenance",
                f"- {total_etfs:,} categorization ETFs processed",
                f"- {total_esg:,} ESG tickers refreshed",
            ]))

            # The week's new Form N-CEN filings on EDGAR -> the list of actively managed ETFs.
            # Nothing else reads it, so it runs last.
            try:
                ncen_stats = ncen.run()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on the SEC active ETF list (N-CEN) with error:\n{e}\n\n")
                raise e

            sections.append(ncen.summary(ncen_stats))

            # Then the listed funds' FMP profiles: the actively managed equity ones go to the provider
            # tables, sized against the index funds' snapshot, and the selection rules set which of
            # them are active (their holdings are downloaded from the next daily run).
            try:
                profile_stats = etf_profile.run()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on the SEC ETF profiles (FMP) with error:\n{e}\n\n")
                raise e

            sections.append(etf_profile.summary(profile_stats))

        if generation:
            # The generators, after the daily steps above (which stored and valued the screen): the
            # listings' free floats and companies' float factors (the note/preferred lines the
            # company builder leaves out, and a check of the market caps against the index funds), then
            # the companies are built from the stored screen and the listings' links, then benchmarks,
            # best ideas, funds — each reading what the previous step stored. A failed step stops
            # the rest: the FMP screener fails after its retries, and no companies or an empty
            # benchmark fails too, so the generators never run on partial data. A failed float
            # download keeps last week's values (logged) rather than stopping the run.
            # First the index funds (VTI, VEA, VWO): their holdings snapshot and the market's size
            # breakpoints - the float factors, benchmark cutoffs and funds' large-cap line use them.
            try:
                index_stats = index_funds.refresh()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on the index funds download with error:\n{e}\n\n")
                raise e

            sections.append(index_funds.summary(index_stats))

            try:
                floats = free_float.refresh()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on the free float refresh with error:\n{e}\n\n")
                raise e

            sections.append(free_float.summary(floats))

            try:
                companies = company_builder.run()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on the company builder with error:\n{e}\n\n")
                raise e

            sections.append(company_builder.summary(companies))

            try:
                bench = benchmark_generator.run()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on benchmark generation with error:\n{e}\n\n")
                raise e

            sections.append(benchmark_generator.summary(bench))

            try:
                etfs_processed, generated_etfs, problems = best_ideas_generator.run()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on best ideas processing with error:\n{e}\n\n")
                raise e

            sections.append("\n".join([
                "Best ideas",
                f"- {etfs_processed:,} ETFs available",
                f"- {generated_etfs:,} with best ideas",
                f"- {len(problems):,} problem(s)" + (":" if problems else ""),
                *(f"  - {p}" for p in problems),
            ]))

            try:
                results = funds_update.run()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on model fund update with error:\n{e}\n\n")
                raise e

            sections += [results_to_string(r) for r in results]

        end = datetime.now(timezone.utc)
        run_lines = [
            f"Cron run {run_day:%a} {run_day}",
            f"- Started {start_time:%H:%M:%S} UTC",
            f"- Completed {end:%H:%M:%S} UTC ({_duration((end - start_time).total_seconds())})",
        ]
        if generation_note:
            run_lines.append(f"- {generation_note}")
        sender.send_admin(subject="Best Ideas Cron Completed", message="\n\n".join(["\n".join(run_lines), *sections]) + "\n")

    except Exception as e:
        log.record_error(f"Error in Best Ideas cron service: {e}")
        raise Exception(f"Cron Job Best Ideas failed - {e}")
    