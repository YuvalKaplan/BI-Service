from datetime import date, timedelta
from modules.bt.object import fund, best_idea, ticker, provider_etf_holding
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

def get_first_weekday_samples(start_date, end_date):
    """
    Finds the first weekday (Mon-Fri) sample for each month 
    based on a 4-day cadence (1, 5, 9, 13...).
    """
    current_date = start_date
    last_processed_month = None
    target_dates = []

    while current_date <= end_date:
        # 1. Check if we are in a new month
        # Use a tuple of (year, month) to ensure it works across multiple years
        current_month_key = (current_date.year, current_date.month)
        
        if current_month_key != last_processed_month:
            # 2. Check if it's a weekday (0-4 is Mon-Fri)
            if current_date.weekday() < 5:
                target_dates.append(current_date)
                # Mark this specific year/month as 'done'
                last_processed_month = current_month_key
        
        # 3. Increment by 4 days to stay synced with your DB (1, 5, 9, 13...)
        current_date += timedelta(days=4)
        
    return target_dates


def run(start_date: date, end_date: date):

    accounts = account.fetch_all()
    etf_ids = distinct_provider_etfs(accounts)
    symbols = provider_etf_holding.fetch_tickers_for_etfs(etf_ids)

    # Stock data gathering
    do_ticker_download_and_prep = True
    if do_ticker_download_and_prep == True:
        # Download all stock information (prices, market cap and dividends)
        stocks_download.run(symbols, start_date - timedelta(days=15), end_date + timedelta(days=15))

        # Mark the stocks as value/growh, based on Value/Growth ETF sources and for not found in ETFs, use the classification model
        ticker.mark_style()
        
        # ticker.mark_split_invalid(symbols, start_date - timedelta(days=5), end_date + timedelta(days=5))

    # Identify the lateset best ideas per ETF.
    do_best_ideas = False
    if do_best_ideas:
        best_idea.reset()

        # Run once a week
        current_sim_date = start_date
        while current_sim_date <= end_date:
            if current_sim_date.weekday() == 2:  # Tuesday
                print(f"Identifying best ideas per ETF on: {current_sim_date.strftime("%A, %d-%m-%Y")}")
                best_ideas_generator.run(etf_ids, current_sim_date)

            current_sim_date += timedelta(days=1)
 
    first_samples = get_first_weekday_samples(start_date, end_date)

    # Construct todays target fund holdings. 
    do_target_fund = True
    if do_target_fund:
        fund.reset_funds()

        # Run once a week
        current_sim_date = start_date
        while current_sim_date <= end_date:
            if current_sim_date.weekday() == 2: # Tuesday
                print(f"Constructing target funds holdings on: {current_sim_date.strftime("%A, %d-%m-%Y")}")
                funds_update.run(current_sim_date)
            current_sim_date += timedelta(days=1)

    # Update account based on daily activity (interest, dividends, transactions, performance)
    do_accounts = True
    if do_accounts:
        account.reset_accounts()

        current_sim_date = start_date
        while current_sim_date <= end_date:
            print(f"Generating account activity on: {current_sim_date.strftime("%A, %d-%m-%Y")}")
            for current_account in accounts:
                account_update.daily_actions(current_account, current_sim_date)
            current_sim_date += timedelta(days=1)

