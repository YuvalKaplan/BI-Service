import atexit
import log
from datetime import datetime, timezone
from modules.object.exit import cleanup
from modules.core.db import db_pool_instance, ENVIRONMENT
from modules.core import sender
from modules.calc.model_fund import results_to_string
from modules.cron import categorize_downloader, etf_downloader, best_ideas_generator, funds_update, benchmark_generator, universe_screener, universe_builder
from modules.ticker import esg, master, refresh, style, valuation

SEPERATOR_LINE = "-" * 20 + "\n"
BREAKER_LINE = "=" * 20 + "\n\n"

atexit.register(cleanup)

if __name__ == '__main__':
    try:
        print("Starting cron service")
        print(f"DB connection pool started with {db_pool_instance.get_max_connections()} connections.")
        
        log.record_status(f"Starting cron service in Environment: {ENVIRONMENT}")

        start_time = datetime.now(timezone.utc)
        weekday = start_time.weekday() # 0 = Monday, 4 = Friday, 6 = Sunday
        message_actions = ""

        if 1 <= weekday <= 5: # Tuesday through Saturday
            # Listings in first — the holdings (stage 2) and the large-cap screener (stage 3), which
            # register their tickers as they read them — then ticker maintenance (stage 4) runs the
            # ticker utilities over every ticker, in order: profiles (so values use current
            # currencies and skip tickers no longer valid) -> values (once per ticker in use; on
            # Wednesday also the stored screen's lines) -> share-class consolidation (company caps
            # come from the values) -> style (after grouping, so a new listing isn't classified on
            # its own). The Wednesday generators only ever use maintained tickers.
            try:
                stats_downloader, total_downloaded, provider_ids, = etf_downloader.run(start_time)
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on holdings download with error:\n{e}\n")
                raise e

            message_actions += f"Holdings Download\n" + SEPERATOR_LINE
            message_actions += f"{stats_downloader}\n" + SEPERATOR_LINE
            message_actions += f"Total ETFs downloaded: {total_downloaded}\n"
            message_actions += BREAKER_LINE

            try:
                screen = universe_screener.run(store=(weekday == 2))
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on the FMP large-cap screener with error:\n{e}\n\n")
                raise e

            message_actions += universe_screener.summary(screen) + "\n"
            message_actions += BREAKER_LINE

            message_actions += f"Ticker Maintenance\n" + SEPERATOR_LINE
            try:
                profiles_checked, profiles_updated, profiles_marked_invalid = refresh.refresh_ticker_profiles()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on ticker profile refresh with error:\n{e}\n")
                raise e

            message_actions += f"Ticker profiles refreshed: {profiles_updated} updated, {profiles_marked_invalid} marked invalid, out of {profiles_checked} checked\n"

            try:
                values = valuation.run()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on ticker values with error:\n{e}\n\n")
                raise e

            message_actions += valuation.summary(values) + "\n"

            try:
                masters_updated, unlinked, caps_updated = master.sync_masters_and_company_data()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on the master ticker sync with error:\n{e}\n\n")
                raise e

            message_actions += f"Ticker master sync: {masters_updated} link(s), {unlinked} unlinked, {caps_updated} company cap(s) refreshed\n"

            try:
                style.assign_styles()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on style assignment with error:\n{e}\n\n")
                raise e

            message_actions += BREAKER_LINE

        if weekday == 6:  # Sunday — ticker maintenance, Sunday part: style reference data, then ESG
            try:
                total_etfs = categorize_downloader.run()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed in categorize ETF download with error:\n{e}\n\n")
                raise e

            message_actions += f"Categorization ETFs processed: {total_etfs}\n"
            message_actions += BREAKER_LINE

            try:
                total_esg = esg.refresh_all()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on ESG update with error:\n{e}\n\n")
                raise e

            message_actions += f"ESG tickers refreshed: {total_esg}\n"
            message_actions += BREAKER_LINE

        if weekday == 2: # Wednesday
            # The generators, after the Tue–Sat steps above (which stored and valued the screen): the
            # universe is built from the stored screen and the linked companies, then benchmarks,
            # best ideas, funds — each reading what the previous step stored. A failed step stops
            # the rest: the FMP screener fails after its retries, and an empty universe or
            # benchmark fails too, so the generators never run on partial data.
            try:
                universe = universe_builder.run()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on the large-cap universe with error:\n{e}\n\n")
                raise e

            message_actions += universe_builder.summary(universe) + "\n"
            message_actions += BREAKER_LINE

            try:
                bench = benchmark_generator.run()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on benchmark generation with error:\n{e}\n\n")
                raise e

            message_actions += f"Benchmark holdings refreshed:\n" + SEPERATOR_LINE
            message_actions += benchmark_generator.summary(bench) + "\n"
            message_actions += BREAKER_LINE

            try:
                etfs_processed, generated_etfs, problems = best_ideas_generator.run()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on best ideas processing with error:\n{e}\n\n")
                raise e

            message_actions += f"Total ETFs available: {etfs_processed}\n"
            message_actions += f"ETFS with best ideas: {generated_etfs}\n"
            message_actions += f"ETFS with problems: {len(problems)}\n" + SEPERATOR_LINE
            for p in problems:
                message_actions += f"{p}\n"
            message_actions += BREAKER_LINE

            try:
                results = funds_update.run()
            except Exception as e:
                sender.send_admin(subject="Best Ideas Cron Failed", message=f"Failed on model fund update with error:\n{e}\n\n")
                raise e

            message_actions += f"Fund Updates:\n" + SEPERATOR_LINE
            for r in results:
                message_actions += f"{results_to_string(r)}\n"
                message_actions += BREAKER_LINE

        end = datetime.now(timezone.utc)
        message_full = f"Activated at {start_time.strftime("%H:%M:%S")}\nCompleted at {end.strftime("%H:%M:%S")}.\n\n"
        sender.send_admin(subject="Best Ideas Cron Completed", message=message_full + message_actions)

    except Exception as e:
        log.record_error(f"Error in Best Ideas cron service: {e}")
        raise Exception(f"Cron Job Best Ideas failed - {e}")
    