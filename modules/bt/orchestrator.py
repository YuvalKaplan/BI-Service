from datetime import date, timedelta
import log
from modules.bt.object import fund, best_idea, ticker, provider_etf_holding, categorize_ticker
from modules.bt.object import account, performance
from modules.bt.actions import stocks_categorize as bt_categorize_tickers, stocks_download, best_ideas_generator, funds_update, account_update
from modules.bt.calc.model_fund import getStrategyFromJson
from modules.bt.calc import classification

CALC_PERIOD_WEEKLY    = "WEEKLY"     # every Wednesday
CALC_PERIOD_MONTHLY   = "MONTHLY"    # 15th of every month
CALC_PERIOD_BIMONTHLY = "BIMONTHLY"  # 15th of every odd month (Jan, Mar, May, Jul, Sep, Nov)
CALC_PERIOD_QUARTERLY = "QUARTERLY"  # 15th of Jan / Apr / Jul / Oct

# --- Configuration ---
START_DATE  = date(2022, 1, 1)
END_DATE    = date(2025, 12, 31)
CALC_PERIOD = CALC_PERIOD_MONTHLY  # CALC_PERIOD_WEEKLY | CALC_PERIOD_MONTHLY | CALC_PERIOD_BIMONTHLY | CALC_PERIOD_QUARTERLY
# ---------------------

def _is_calc_date(d: date, calc_period: str) -> bool:
    if calc_period == CALC_PERIOD_WEEKLY:
        return d.weekday() == 2  # Wednesday
    if calc_period == CALC_PERIOD_QUARTERLY:
        return d.day == 15 and d.month in (1, 4, 7, 10)
    if calc_period == CALC_PERIOD_BIMONTHLY:
        return d.day == 15 and d.month % 2 == 1
    return d.day == 15  # MONTHLY (default)


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

def run():
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
        stocks_download.run(symbols, START_DATE - timedelta(days=15), END_DATE + timedelta(days=15))

        bt_categorize_tickers.download_data()

        categorized_tickers = [classification.to_categorize_ticker_item(t) for t in categorize_ticker.fetch_all_for_style_classification()]
        classifier = classification.get_classifier(categorized_tickers)
        ticker.mark_style(classifier)

        # ticker.mark_split_invalid(symbols, START_DATE - timedelta(days=5), END_DATE + timedelta(days=5))

    # Identify the lateset best ideas per ETF.
    if do_best_ideas:
        best_idea.reset()

        current_sim_date = START_DATE
        while current_sim_date <= END_DATE:
            if _is_calc_date(current_sim_date, CALC_PERIOD):
                print(f"Identifying best ideas per ETF on: {current_sim_date.strftime("%A, %d-%m-%Y")}")
                best_ideas_generator.run(etf_ids, current_sim_date)
            current_sim_date += timedelta(days=1)

    # Construct todays target fund holdings.
    if do_target_fund:
        fund.reset_funds()

        current_sim_date = START_DATE
        while current_sim_date <= END_DATE:
            if _is_calc_date(current_sim_date, CALC_PERIOD):
                print(f"Constructing target funds holdings on: {current_sim_date.strftime("%A, %d-%m-%Y")}")
                funds_update.run(current_sim_date)
            current_sim_date += timedelta(days=1)

    # Update account based on daily activity (interest, dividends, transactions, performance)
    if do_accounts:
        account.reset_accounts()

        current_sim_date = START_DATE
        while current_sim_date <= END_DATE:
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

