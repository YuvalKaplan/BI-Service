from datetime import date, timedelta
import log
from modules.bt.object import fund, best_idea, ticker, provider_etf_holding, categorize_ticker
from modules.bt.object import account, performance
from modules.bt.actions import stocks_download, best_ideas_generator, funds_update, account_update
from modules.calc.model_fund import getStrategyFromJson
from modules.calc import classification

# Strategy frequency: 7 for weekly, 14 for bi-weekly
STRATEGY_RUN_INTERVAL_DAYS = 7

def distinct_provider_etfs(accounts) -> list[int]:
    distinct_etfs = set()
    for current_account in accounts:
        f = fund.fetch_fund(current_account.strategy_fund_id)
        if f is None:
            raise Exception("Missing strategy for fund")

        strategy = getStrategyFromJson(f.strategy)
        strategy_etfs = strategy.provider_etfs
        if strategy_etfs:
            distinct_etfs.update(strategy_etfs)

    return list(distinct_etfs)

def run(start_date: date, end_date: date):
    accounts = account.fetch_all()
    etf_ids = distinct_provider_etfs(accounts)
    
    do_data_download = False
    do_best_ideas = False
    do_target_fund = True
    do_accounts = True

    # Stock data gathering
    if do_data_download:
        ticker.sanitize()
        symbols = provider_etf_holding.fetch_tickers_for_etfs(etf_ids)

        # Download all stock information (prices, market cap and dividends)
        stocks_download.run(symbols, start_date - timedelta(days=15), end_date + timedelta(days=15))

        last_update = categorize_ticker.fetch_last_update()
        last_update_date = last_update.date() if last_update else None
        if last_update_date is None or (date.today() - last_update_date) > timedelta(days=90):
            raise Exception("Table categorize_ticker data is stale or empty — sync it from the live DB before running the back test.")

        categorized_tickers = [classification.to_categorize_ticker_item(t) for t in categorize_ticker.fetch_all_for_style_classification()]
        classifier = classification.get_classifier(categorized_tickers)
        ticker.mark_style(classifier)

        esg_tickers = categorize_ticker.fetch_all_for_esg()
        if esg_tickers:
            ticker.update_esg_qualified([t.symbol for t in esg_tickers])

        # ticker.mark_split_invalid(symbols, start_date - timedelta(days=5), end_date + timedelta(days=5))

    # Identify the lateset best ideas per ETF.
    if do_best_ideas:
        best_idea.reset()

        # Run on interval (weekly or bi-weekly based on STRATEGY_RUN_INTERVAL_DAYS)
        last_run_date = None
        current_sim_date = start_date
        while current_sim_date <= end_date:
            if current_sim_date.weekday() == 2:  # Tuesday
                if last_run_date is None or (current_sim_date - last_run_date).days >= STRATEGY_RUN_INTERVAL_DAYS:
                    print(f"Identifying best ideas per ETF on: {current_sim_date.strftime("%A, %d-%m-%Y")}")
                    best_ideas_generator.run(etf_ids, current_sim_date)
                    last_run_date = current_sim_date

            current_sim_date += timedelta(days=1)

    # Construct todays target fund holdings. 
    if do_target_fund:
        fund.reset_funds()

        # Run on interval (weekly or bi-weekly based on STRATEGY_RUN_INTERVAL_DAYS)
        last_run_date = None
        current_sim_date = start_date
        while current_sim_date <= end_date:
            if current_sim_date.weekday() == 2: # Tuesday
                if last_run_date is None or (current_sim_date - last_run_date).days >= STRATEGY_RUN_INTERVAL_DAYS:
                    print(f"Constructing target funds holdings on: {current_sim_date.strftime("%A, %d-%m-%Y")}")
                    funds_update.run(current_sim_date)
                    last_run_date = current_sim_date
            current_sim_date += timedelta(days=1)

    # Update account based on daily activity (interest, dividends, transactions, performance)
    if do_accounts:
        account.reset_accounts()

        current_sim_date = start_date
        while current_sim_date <= end_date:
            print(f"Generating account activity on: {current_sim_date.strftime("%A, %d-%m-%Y")}")
            for current_account in accounts:
                account_update.daily_actions(current_account, current_sim_date)
            current_sim_date += timedelta(days=1)

    # Results
    alpha_rows = performance.fetch_alpha_annual()
    header = f"{'Year':<8}{'Account':<6}{'Benchmark':<12}{'Strategy':>10}{'Benchmark':>12}{'Alpha':>10}"
    log.record_status(header)
    log.record_status("-" * len(header))
    for row in alpha_rows:
        log.record_status(
            f"{int(row.performance_year):<8}{row.account_id:<6}{row.benchmark_symbol:<12}"
            f"{float(row.annual_strategy_return):>10.2%}{float(row.annual_benchmark_return):>12.2%}"
            f"{float(row.annual_alpha):>10.2%}"
        )

    for current_account in accounts:
        if current_account.id is not None:
            f = fund.fetch_fund(current_account.strategy_fund_id)
            fund_name = f.name if f else str(current_account.strategy_fund_id)
            file_name = f"{current_account.name} - {fund_name}"
            performance.export_daily_returns_csv(current_account.id, file_name)

