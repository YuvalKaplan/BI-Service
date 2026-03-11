import pandas as pd
from decimal import Decimal
from typing import List, Tuple
from decimal import Decimal
from dataclasses import dataclass
from datetime import date, timedelta
from modules.bt.object import fund, fund_holding
from modules.bt.object import account, account_holding as ah, account_cash_ledger as acl, account_trade as at, account_performance as ap, account_benchmark_comparison as abc
from modules.bt.object import benchmark_value as bv
from modules.bt.object import interest_config, ticker, ticker_value, ticker_dividend_history
from modules.bt.actions import stocks_download, best_ideas_generator, funds_update

# def process_daily_cash_accruals(account_id: int, sim_date: date):
#     # 1. Interest
#     balance = acl.get_cash_balance(account_id, sim_date)
#     if balance > 0:
#         rate_cfg = interest_config.get_latest_interest_rate(sim_date)
#         if rate_cfg:
#             daily_int = (balance * (rate_cfg.annual_rate / Decimal('365'))).quantize(Decimal('0.01'))
#             acl.record_cash_transaction(acl.AccountCashLedger(
#                 account_id=account_id,
#                 transaction_date=sim_date,
#                 amount=daily_int,
#                 entry_type='INTEREST',
#                 description=f"Interest on {balance:,.2f}"
#             ))

#     # 2. Dividends
#     divs = ticker_dividend_history.fetch_dividends_for_holdings(account_id, sim_date)
#     for d in divs:
#         total = (d['quantity'] * d['amount_per_share']).quantize(Decimal('0.01'))
#         acl.record_cash_transaction(acl.AccountCashLedger(
#             account_id=account_id,
#             transaction_date=sim_date,
#             amount=total,
#             entry_type='DIVIDEND',
#             description=f"Div: {d['symbol']} ({d['quantity']} shares)"
#         ))

# @dataclass
# class TradeCandidate:
#     symbol: str
#     action: str  # 'ENTER', 'EXIT', 'DRIFT'
#     priority: int
#     current_qty: Decimal = Decimal('0')

# def identify_rebalance_needs(account_id: int, fund_id: int, eval_date: date) -> Tuple[List[str], List[ah.AccountHolding], List[TradeCandidate]]:
#     """
#     Compares current account holdings vs fund targets.
#     Returns: (target_map, list_of_candidates)
#     """
#     try:
#         # Fetch targets from your fund_holding module
#         base_fund_holding = fund_holding.fetch_funds_holdings(fund_id=fund_id, eval_date=eval_date)
#         target_symbols = [h.symbol for h in base_fund_holding]
#         targets_holdings = {h.symbol: h for h in base_fund_holding}

#         # Fetch current snapshot
#         account_holdings = ah.fetch_current_account_snapshot(account_id=account_id, eval_date=eval_date)
#         current_holdings = {h.symbol: h for h in account_holdings}

#         all_symbols = set(targets_holdings.keys()) | set(current_holdings.keys())
#         trade_candidates = []

#         for sym in all_symbols:
#             target = targets_holdings.get(sym)
#             actual = current_holdings.get(sym)

#             if target and not actual:
#                 trade_candidates.append(TradeCandidate(
#                     symbol=sym, action="ENTER", priority=target.ranking
#                 ))
#             elif actual and not target:
#                 trade_candidates.append(TradeCandidate(
#                     symbol=sym, action="EXIT", priority=0, 
#                     current_qty=Decimal(str(actual.quantity))
#                 ))
#             elif actual and target:
#                 # Optional: Logic for drift rebalance can be added here
#                 pass 

#         return target_symbols, account_holdings, trade_candidates

#     except Exception as e:
#         print(f"Error comparing holdings: {e}")
#         return [], [], []

# def calculate_commission(quantity: Decimal) -> Decimal:
#     FEE_PER_SHARE = Decimal('0.005') # e.g., half a cent per share
#     MIN_COMMISSION = Decimal('0.00')  # Minimum charge per order
    
#     fee = quantity * FEE_PER_SHARE
#     return max(fee, MIN_COMMISSION).quantize(Decimal('0.01'))

# def execute_minimal_rebalance(account_id: int, num_fund_holdings: int, candidates: List[TradeCandidate], current_sim_date: date) -> List[at.AccountTrade]:
#     if not candidates:
#         return []

#     # Fetch Data
#     cash_val = acl.get_cash_balance(account_id, current_sim_date)
#     current_holdings = ah.fetch_current_account_snapshot(account_id, current_sim_date)
    
#     # Get all unique symbols involved (Holdings + Candidates)
#     all_syms = list(set([h.symbol for h in current_holdings] + [c.symbol for c in candidates]))
#     ticker_data = ticker_value.fetch_tickers_by_symbols_on_date(all_syms, current_sim_date)
#     prices = {t.symbol: Decimal(str(t.stock_price)) for t in ticker_data if t.stock_price}

#     # Build the Rebalance DataFrame
#     df = pd.DataFrame([{'symbol': s} for s in all_syms])
    
#     # Map quantities and prices
#     holdings_map = {h.symbol: Decimal(str(h.quantity)) for h in current_holdings}
#     df['qty_held'] = df['symbol'].map(holdings_map).fillna(0.0)
#     df['price'] = df['symbol'].map(prices).fillna(0.0)
#     df['current_val'] = df['qty_held'] * df['price']
    
#     tpv = cash_val + df['current_val'].sum()
#     target_val_per_stock = tpv / num_fund_holdings if num_fund_holdings > 0 else 0.0

#     # Target Logic
#     candidate_map = {c.symbol: c.action for c in candidates}
#     def get_target(sym):
#         action = candidate_map.get(sym)
#         if action == 'EXIT': return 0.0
#         if action == 'ENTER': return target_val_per_stock
#         return target_val_per_stock if sym in [h.symbol for h in current_holdings] else 0.0

#     df['target_val'] = df['symbol'].map(get_target)
#     df['delta'] = df['current_val'] - df['target_val']
#     df['abs_delta'] = df['delta'].abs()
#     df = df.sort_values('abs_delta', ascending=False)


#     # --- PHASE 2: CALCULATE ORDERS (No DB calls here) ---
#     trades: List[at.AccountTrade] = []
#     cash_ledger_entries: List[acl.AccountCashLedger] = []
    
#     # Track cash locally as we "spend" it in the simulation
#     local_cash = Decimal(str(cash_val))

#     # SELLS FIRST (to generate cash)
#     for _, row in df[df['delta'] > 1.0].iterrows():
#         d_price = Decimal(str(row['price']))
#         d_delta = Decimal(str(row['delta']))
#         d_qty = (d_delta / d_price).quantize(Decimal('0.00000001'))
#         d_qty = min(d_qty, Decimal(str(row['qty_held']))) # Safety

#         gross_amount = d_qty * d_price
#         commission = calculate_commission(d_qty)
#         net_proceeds = gross_amount - commission

#         trades.append(at.AccountTrade(
#             account_id=account_id, symbol=row['symbol'], trade_date=current_sim_date,
#             side='SELL', quantity=d_qty, price=d_price, commission=commission, total_amount=net_proceeds
#         ))
#         cash_ledger_entries.append(acl.AccountCashLedger(
#             account_id=account_id, transaction_date=current_sim_date, 
#             amount=net_proceeds, entry_type='TRADE_SELL', description=f"Sold {row['symbol']} (Fee: ${commission})"
#         ))
#         local_cash += net_proceeds

#     # BUYS NEXT (constrained by local_cash)
#     for _, row in df[df['delta'] < -1.0].iterrows():
#         # Quick check if we can even afford a base fee
#         if local_cash <= Decimal('2.00'): break 
        
#         d_price = Decimal(str(row['price']))
#         target_gross = Decimal(str(abs(row['delta'])))
        
#         # Estimate commission to ensure we don't overspend cash
#         # Since Cash = Gross + (Base + Gross * %)... 
#         # Gross = (Cash - Base) / (1 + %)
#         max_gross = (local_cash - Decimal('1.00')) / Decimal('1.001')
        
#         actual_gross = min(target_gross, max_gross)
#         d_qty = (actual_gross / d_price).quantize(Decimal('0.00000001'))

        
#         if d_qty  > 0:
#             final_gross  = d_qty  * d_price
#             commission = calculate_commission(d_qty)
#             total_cost = final_gross + commission

#             trades.append(at.AccountTrade(
#                 account_id=account_id, symbol=row['symbol'], trade_date=current_sim_date,
#                 side='BUY', quantity=d_qty, price=d_price, commission=commission, total_amount=final_gross
#             ))
#             cash_ledger_entries.append(acl.AccountCashLedger(
#                 account_id=account_id, transaction_date=current_sim_date, 
#                 amount=-total_cost, entry_type='TRADE_BUY', description=f"Bought {row['symbol']} (Fee: ${commission})"
#             ))
#             local_cash -= total_cost

#     # --- PHASE 3: COMMIT ---
#     for trade in trades:
#         at.record_trade(trade)
    
#     for ledger in cash_ledger_entries:
#         acl.record_cash_transaction(ledger)

#     return trades

# def create_daily_snapshot(
#     account_id: int, 
#     eval_date: date, 
#     previous_holdings: List[ah.AccountHolding], # Already in memory
#     today_trades: List[at.AccountTrade]         # Returned from rebalance
# ) -> tuple[Decimal, List[ah.AccountHolding]] :
    
#     # 1. Load data into DataFrames
#     df_prev = pd.DataFrame([vars(h) for h in previous_holdings])
#     if df_prev.empty:
#         df_prev = pd.DataFrame(columns=['symbol', 'quantity', 'cost_basis'])
    
#     df_trades = pd.DataFrame([vars(t) for t in today_trades])

#     # 2. Process Trades (Aggregate by Symbol)
#     if not df_trades.empty:
#         # Net Quantity: BUY is +, SELL is -
#         df_trades['qty_delta'] = df_trades.apply(
#             lambda x: float(x['quantity']) if x['side'] == 'BUY' else -float(x['quantity']), axis=1
#         )
#         # Cost Basis Delta: Only BUYS increase cost basis (Price + Fee)
#         df_trades['cost_delta'] = df_trades.apply(
#             lambda x: float(x['total_amount'] + x['commission']) if x['side'] == 'BUY' else 0.0, axis=1
#         )
#         # Track total quantity sold for pro-rata cost reduction
#         df_trades['qty_sold'] = df_trades.apply(
#             lambda x: float(x['quantity']) if x['side'] == 'SELL' else 0.0, axis=1
#         )
        
#         trade_summary = df_trades.groupby('symbol').agg({
#             'qty_delta': 'sum',
#             'cost_delta': 'sum',
#             'qty_sold': 'sum'
#         }).reset_index()
#     else:
#         trade_summary = pd.DataFrame(columns=['symbol', 'qty_delta', 'cost_delta', 'qty_sold'])

#     # 3. Merge Yesterday and Today
#     df = pd.merge(
#         df_prev[['symbol', 'quantity', 'cost_basis']], 
#         trade_summary, 
#         on='symbol', 
#         how='outer'
#     ).fillna(0.0)
    
#     # 4. Calculate New Quantity
#     df['new_qty'] = df['quantity'] + df['qty_delta']
    
#     # 5. Calculate Adjusted Cost Basis
#     # For Sells: Reduce cost basis by the % of the position sold
#     def calculate_new_cost(row):
#         old_qty = float(row['quantity'])
#         old_cost = float(row['cost_basis'])
#         qty_sold = float(row['qty_sold'])
        
#         if old_qty > 0 and qty_sold > 0:
#             sell_pct = min(1.0, qty_sold / old_qty)
#             old_cost = old_cost * (1.0 - sell_pct)
            
#         return old_cost + float(row['cost_delta'])

#     df['new_cost'] = df.apply(calculate_new_cost, axis=1)
    
#     # Filter for active holdings (remove positions closed today)
#     df = df[df['new_qty'] > 1e-8].copy()
#     if df.empty: return []

#     # 6. Market Value & Weights
#     prices_raw = ticker_value.fetch_tickers_by_symbols_on_date(df['symbol'].tolist(), eval_date)
#     price_map = {p.symbol: float(p.stock_price) for p in prices_raw if p.stock_price}
    
#     df['price'] = df['symbol'].map(price_map).fillna(0.0)
#     df['mkt_val'] = df['new_qty'] * df['price']
    
#     # Cash at End of Day
#     eod_cash = float(acl.get_cash_balance(account_id, eval_date + timedelta(days=1)))
#     tpv = df['mkt_val'].sum() + eod_cash
#     df['weight'] = df['mkt_val'] / tpv if tpv > 0 else 0.0

#     snapshots = [
#         ah.AccountHolding(
#             account_id=account_id,
#             holding_date=eval_date,
#             symbol=row['symbol'],
#             quantity=Decimal(str(row['new_qty'])),
#             cost_basis=Decimal(str(row['new_cost'])),
#             market_value=Decimal(str(row['mkt_val'])),
#             weight_percentage=Decimal(str(row['weight']))
#         ) for _, row in df.iterrows()
#     ]

#     # 7. Record into database
#     ah.record_account_holdings(snapshots)
#     daily_return = ap.record_daily_performance(account_id=account_id, eval_date=eval_date, cash_balance=Decimal(eod_cash), snapshots=snapshots)

#     return daily_return, snapshots


# def benchmark_comparison(account_id: int, fund_id: int, eval_date: date, daily_return: Decimal, snapshots: List[ah.AccountHolding]):
#     fund_data = fund.fetch_fund(fund_id) # Using your existing fetch_fund

#     if (fund_data is None):
#         raise Exception("Account has not been assoicated a fund strategy")

#     strategy = fund.getStrategyFromJson(fund_data.strategy)
    
#     if not strategy or not strategy.benchmarks:
#         return

#     for symbol in strategy.benchmarks:
#         # Get Benchmark Prices
#         today_bench = bv.fetch_benchmark_price(symbol, eval_date)
#         prev_bench = bv.fetch_latest_benchmark_price_before(symbol, eval_date)

#         if not today_bench or not prev_bench:
#             continue

#         # Calculate Benchmark Metrics
#         bench_return = (today_bench.price / prev_bench.price) - 1
#         alpha = daily_return - bench_return

#         # Calculate Indexed Growth ($1.00 starting value)
#         prev_strat_idx, prev_bench_idx = abc.fetch_previous_comparison_values(account_id, symbol, eval_date)
        
#         new_strat_idx = (prev_strat_idx * (1 + daily_return)).quantize(Decimal('0.000001'))
#         new_bench_idx = (prev_bench_idx * (1 + bench_return)).quantize(Decimal('0.000001'))

#         # Save Comparison
#         abc.record_benchmark_comparison(abc.AccountBenchmarkComparison(
#             account_id=account_id,
#             benchmark_symbol=symbol,
#             performance_date=eval_date,
#             strategy_indexed_value=new_strat_idx,
#             benchmark_indexed_value=new_bench_idx,
#             daily_alpha=alpha.quantize(Decimal('0.000001'))
#         ))

# def update_account(account: account.Account, current_sim_date: date):

#     # Update Cash: Apply interest and record dividends 
#     process_daily_cash_accruals(account.id, current_sim_date)
    
#     # Check for changes in target fund_holdings
#     fund_symbols, account_holdings, needs = identify_rebalance_needs(account.id, account.strategy_fund_id, current_sim_date)
    
#     # Execute the changes and rebalance
#     today_trades = execute_minimal_rebalance(account.id, len(fund_symbols), needs, current_sim_date)

#     # Create Today's Snapshot
#     daily_return, snapshots = create_daily_snapshot(account_id=account.id, eval_date=current_sim_date, previous_holdings=account_holdings, today_trades=today_trades)
    
#     # Record Performance
#     benchmark_comparison(account.id, account.strategy_fund_id, current_sim_date, daily_return, snapshots)


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
    current_sim_date = start_date
    accounts = account.fetch_all()
    etf_ids = distinct_provider_etfs(accounts)

    # stocks_download.run(etf_ids, start_date - timedelta(days=15), end_date + timedelta(days=15))
    ticker.mark_categories()

    while current_sim_date <= end_date:
        
        if 1 <= current_sim_date.weekday() <= 5: # Tuesday through Saturday
            print(f"Processing: {current_sim_date.strftime("%A, %d-%m-%Y")}")

            # Identify the lateset best ideas and construct todays target fund holdings.
            etfs_processed, generated_etfs, problems = best_ideas_generator.run(etf_ids, current_sim_date)
            results = funds_update.run(current_sim_date)

    #     for current_account in accounts:
    #         update_account(current_account, current_sim_date)

        current_sim_date += timedelta(days=1)

