from datetime import date, timedelta
from modules.bt.object import fund, ticker, provider_etf_holding
from modules.bt.object import account
from modules.bt.actions import stocks_download, best_ideas_generator, funds_update, account_update

def distinct_provider_etfs(accounts) -> list[int]:
    distinct_etfs = set()
    for current_account in accounts:
        f = fund.fetch_fund(current_account.strategy_fund_id)
        if f is None:
            raise Exception("Missing strategy for fund")

        strategy = fund.getStrategyFromJson(f.strategy)
        strategy_etfs = strategy.provider_etfs
        if strategy_etfs:
            distinct_etfs.update(strategy_etfs)

    return list(distinct_etfs)

def run(start_date: date, end_date: date):
    fund.reset_funds()
    account.reset_accounts()

    accounts = account.fetch_all()
    etf_ids = distinct_provider_etfs(accounts)
    symbols = provider_etf_holding.fetch_tickers_for_etfs(etf_ids)

    # Download all stock information (prices, market cap and dividends)
    # stocks_download.run(symbols, start_date - timedelta(days=15), end_date + timedelta(days=15))

    # Mark the stocks as value/growh, based on Value/Growth ETF sources
    # ticker.mark_categories()
    # ticker.mark_split_invalid(symbols, start_date - timedelta(days=5), end_date + timedelta(days=5))

    current_sim_date = start_date
    while current_sim_date <= end_date:
        print(f"Processing: {current_sim_date.strftime("%A, %d-%m-%Y")}")

        if 1 <= current_sim_date.weekday() <= 5: # Tuesday through Saturday
            # Identify the lateset best ideas and construct todays target fund holdings.
            etfs_processed, generated_etfs, problems = best_ideas_generator.run(etf_ids, current_sim_date)
            results = funds_update.run(current_sim_date)

        # Update account based on daily activity (interest, dividends, transactions)
        for current_account in accounts:
            account_update.daily_actions(current_account, current_sim_date)

        current_sim_date += timedelta(days=1)

