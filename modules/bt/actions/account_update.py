import pandas as pd
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import List, Tuple
from decimal import Decimal
from dataclasses import dataclass
from datetime import date, timedelta
from modules.bt.object import fund, fund_holding
from modules.calc.model_fund import getStrategyFromJson
from modules.bt.object import account, account_holding as ah, account_cash_ledger as acl, account_trade as at, account_performance as ap, account_benchmark_comparison as abc
from modules.bt.object import benchmark_value as bv
from modules.bt.object import interest_config, ticker_value, ticker_dividend_history

DRIFT_THRESHOLD = 0.05   # 5%

def process_daily_interest(account_id: int, eval_date: date):
    balance = acl.get_cash_balance(account_id, eval_date + timedelta(days=1))
    if balance > 0:
        rate_cfg = interest_config.get_latest_interest_rate(eval_date)
        if rate_cfg:
            daily_int = (balance * (rate_cfg.annual_rate / Decimal('100') / Decimal('365'))).quantize(Decimal('0.01'))
            acl.record_cash_transaction(acl.AccountCashLedger(
                account_id=account_id,
                transaction_date=eval_date,
                amount=daily_int,
                entry_type='INTEREST',
                description=f"Interest on {balance:,.2f}"
            ))

def process_daily_dividends(account_id: int, eval_date: date):
    divs = ticker_dividend_history.fetch_dividends_for_holdings(account_id, eval_date)
    for d in divs:
        total = (d['quantity'] * d['amount_per_share']).quantize(Decimal('0.01'))
        acl.record_cash_transaction(acl.AccountCashLedger(
            account_id=account_id,
            transaction_date=eval_date,
            amount=total,
            entry_type='DIVIDEND',
            description=f"Div: {d['symbol']} ({d['quantity']} shares)"
        ))

@dataclass
class TradeCandidate:
    symbol: str
    action: str  # 'ENTER', 'EXIT', 'DRIFT'
    ranking: int
    current_qty: Decimal = Decimal('0')
    max_delta: float | None = None
    priority: float = 0.0

def identify_position_change_needs(
    account_id: int, 
    fund_id: int, 
    eval_date: date,
    account_holdings: List[ah.AccountHolding]
) -> Tuple[List[str], List[TradeCandidate]]:
    """
    Compares current account holdings vs fund targets.
    Returns: (target_map, list_of_candidates)
    """
    try:
        # Fetch targets from your fund_holding module
        base_fund_holding = fund_holding.fetch_funds_holdings(fund_id=fund_id, eval_date=eval_date)
        target_symbols = [h.symbol for h in base_fund_holding]
        targets_holdings = {h.symbol: h for h in base_fund_holding}

        # Fetch current snapshot
        current_holdings = {h.symbol: h for h in account_holdings}

        all_symbols = set(targets_holdings.keys()) | set(current_holdings.keys())
        candidates = []

        for symbol in all_symbols:

            target = targets_holdings.get(symbol)
            actual = current_holdings.get(symbol)

            if target and not actual:
                candidates.append(TradeCandidate(
                    symbol=symbol, action="ENTER", ranking=target.ranking, max_delta=target.max_delta
                ))
            elif actual and not target:
                candidates.append(TradeCandidate(
                    symbol=symbol, action="EXIT", ranking=0, 
                    current_qty=Decimal(str(actual.quantity))
                ))
            elif actual and target:
                # Optional: Logic for drift rebalance can be added here
                pass 

        # Sort ENTER candidates by ranking (ascending) then by max_delta (descending)
        enter_candidates = [c for c in candidates if c.action == "ENTER"]
        enter_candidates.sort(key=lambda c: (c.ranking, -(c.max_delta if c.max_delta is not None else 0)))

        # Assign priority based on sort order
        for idx, candidate in enumerate(enter_candidates):
            candidate.priority = float(idx)

        other_candidates = [c for c in candidates if c.action != "ENTER"]
        candidates = enter_candidates + other_candidates

        return target_symbols, candidates

    except Exception as e:
        print(f"Error comparing holdings: {e}")
        return [], []

def calculate_commission(quantity: Decimal) -> Decimal:
    FEE_PER_SHARE = Decimal('0.005') # e.g., half a cent per share
    MIN_COMMISSION = Decimal('0.00')  # Minimum charge per order
    
    fee = quantity * FEE_PER_SHARE
    return max(fee, MIN_COMMISSION).quantize(Decimal('0.01'))

def to_price(value) -> Decimal:
    return Decimal(str(value)).quantize(Decimal('0.01'))

def execute_trade(
    account_id: int,
    symbol: str,
    side: str,
    qty: Decimal,
    price: Decimal,
    trade_date: date,
    local_cash: Decimal,
    trades: List[at.AccountTrade],
    cash_entries: List[acl.AccountCashLedger],
    description: str
) -> Decimal:

    if qty <= 0 or price <= 0:
        return local_cash

    gross = (qty * price).quantize(Decimal('0.01'))
    commission = calculate_commission(qty)

    if side == "BUY":

        total_cost = (gross + commission).quantize(Decimal('0.01'))

        if total_cost > local_cash:
            return local_cash

        trades.append(at.AccountTrade(
            account_id=account_id,
            symbol=symbol,
            trade_date=trade_date,
            side='BUY',
            quantity=qty,
            price=price,
            commission=commission,
            total_amount=gross
        ))

        cash_entries.append(acl.AccountCashLedger(
            account_id=account_id,
            transaction_date=trade_date,
            amount=-total_cost,
            entry_type='TRADE_BUY',
            description=description
        ))

        return local_cash - total_cost

    else:  # SELL

        net = (gross - commission).quantize(Decimal('0.01'))

        trades.append(at.AccountTrade(
            account_id=account_id,
            symbol=symbol,
            trade_date=trade_date,
            side='SELL',
            quantity=qty,
            price=price,
            commission=commission,
            total_amount=gross
        ))

        cash_entries.append(acl.AccountCashLedger(
            account_id=account_id,
            transaction_date=trade_date,
            amount=net,
            entry_type='TRADE_SELL',
            description=description
        ))

        return local_cash + net

def execute_minimal_rebalance(
    account_id: int,
    num_strategy_holdings: int,
    candidates: List[TradeCandidate],
    eval_date: date,
    account_holdings: List[ah.AccountHolding]
) -> List[at.AccountTrade]:

    if not candidates:
        return []
   
    # --- FETCH LATEST INDIVIDUAL PRICE DATA ---
    all_syms = list(set([h.symbol for h in account_holdings] + [c.symbol for c in candidates]))
    prices = {}
    quantities = {}
    sync_dates = {}

    for symbol in all_syms:

        # Determine if we already hold this (to decide which date logic to use)
        is_held = any(h.symbol == symbol and h.quantity > 0 for h in account_holdings)

        if is_held:
            # 1. EXISTING: Must have a common date to safely Sell/Rebalance
            sync_date = ah.fetch_latest_common_date_for_ticker(account_id, symbol, eval_date)
        else:
            # 2. NEW BUY: Only needs the latest available price date
            sync_date = ticker_value.fetch_latest_price_date_for_ticker(symbol, eval_date)
        
        if sync_date:
            tv = ticker_value.fetch_ticker_on_date(symbol, sync_date)
            # Only fetch holding if it's an existing position
            holding_at_date = ah.fetch_account_holding_on_date(account_id, symbol, sync_date) if is_held else None
            
            if tv and tv.stock_price:
                prices[symbol] = float(tv.stock_price)
                quantities[symbol] = float(holding_at_date.quantity) if holding_at_date else 0.0
                sync_dates[symbol] = sync_date
                continue

        # Fallback if no date or price found
        prices[symbol] = 0.0
        quantities[symbol] = 0.0
        sync_dates[symbol] = None

    # --- BUILD DATAFRAME ---
    df = pd.DataFrame({'symbol': all_syms})
    df['qty_held'] = df['symbol'].map(quantities).fillna(0.0)
    df['price'] = df['symbol'].map(prices).fillna(0.0)
    df['sync_date'] = df['symbol'].map(sync_dates)
    df['current_val'] = df['qty_held'] * df['price']

    cash_val = float(acl.get_cash_balance(account_id, eval_date))
    tpv = cash_val + df['current_val'].sum()
    target_val_per_stock = tpv / num_strategy_holdings if num_strategy_holdings > 0 else 0.0

    # --- TARGET LOGIC ---
    candidate_map = {c.symbol: c.action for c in candidates}
    sell_symbols = [c.symbol for c in candidates if c.action == "EXIT"]
    buy_symbols = [c.symbol for c in candidates if c.action == "ENTER"]
    enter_priority_map = {c.symbol: c.priority for c in candidates if c.action == "ENTER"}

    # Use the keys from your synced quantities dictionary where qty > 0
    held_symbols = {sym for sym, qty in quantities.items() if qty > 0}

    def get_target(sym):
        action = candidate_map.get(sym)
        if action == 'EXIT':
            return 0.0
        elif action == 'ENTER':
            return target_val_per_stock
        elif sym in held_symbols:
            return target_val_per_stock
        return 0.0

    df['target_val'] = df['symbol'].map(get_target)
    df['delta'] = df['current_val'] - df['target_val']
    df['abs_delta'] = df['delta'].abs()
    
    # Sort: ENTER candidates by priority, others by abs_delta and symbol
    df['_sort_priority'] = df['symbol'].map(enter_priority_map)
    df['_is_enter'] = df['symbol'].isin(enter_priority_map.keys())
    df = df.sort_values(
        by=['_is_enter', '_sort_priority', 'abs_delta', 'symbol'],
        ascending=[False, True, False, True],
        na_position='last'
    )
    df = df.drop(columns=['_is_enter', '_sort_priority'])

    trades: List[at.AccountTrade] = []
    cash_ledger_entries: List[acl.AccountCashLedger] = []

    local_cash = Decimal(str(cash_val)).quantize(Decimal('0.01'))

    # --- SELLS FIRST (EXIT POSITIONS) ---
    for _, row in df[df['symbol'].isin(sell_symbols)].iterrows():
        
        price = to_price(row['price'])
        held_qty = Decimal(str(row['qty_held'])).quantize(Decimal('1'), rounding=ROUND_DOWN)
        
        local_cash = execute_trade(
            account_id,
            row['symbol'],
            "SELL",
            held_qty,
            price,
            eval_date,
            local_cash,
            trades,
            cash_ledger_entries,
            f"Sold {held_qty} units of {row['symbol']}"
        )

    # --- BUYS SECOND (ENTER POSITIONS) ---
    if buy_symbols:
        for _, row in df[df['symbol'].isin(buy_symbols)].iterrows():

            if target_val_per_stock <= Decimal('50.00'):
                break

            price = to_price(row['price'])
            target_val = Decimal(str(row['target_val']))
            if target_val <= Decimal('0'):
                continue

            # Use target value from get_target
            gross_target_value = min(target_val, local_cash)
            if gross_target_value <= Decimal('0'):
                break

            gross_qty = (gross_target_value / price).quantize(Decimal('1'), rounding=ROUND_DOWN)
            if gross_qty <= 0:
                continue

            commission = calculate_commission(gross_qty)
            net_target_value = gross_target_value - commission
            if net_target_value <= Decimal('0'):
                continue

            net_qty = (net_target_value / price).quantize(Decimal('1'), rounding=ROUND_DOWN)
            qty = net_qty
            if qty <= 0:
                continue

            local_cash = execute_trade(
                account_id,
                row['symbol'],
                "BUY",
                qty,
                price,
                eval_date,
                local_cash,
                trades,
                cash_ledger_entries,
                f"Bought {qty} units of {row['symbol']}"
            )

    # --- RECOMPUTE PORTFOLIO AFTER MANDATORY TRADES ---
    df['current_val'] = df['qty_held'] * df['price']

    tpv = float(local_cash) + df['current_val'].sum()

    target_val_per_stock = tpv / num_strategy_holdings if num_strategy_holdings > 0 else 0.0

    df['target_val'] = df['symbol'].map(get_target)

    df['delta'] = df['current_val'] - df['target_val']
    df['abs_delta'] = df['delta'].abs()

    df['drift_pct'] = 0.0
    mask = df['target_val'] > 0

    df.loc[mask, 'drift_pct'] = df.loc[mask, 'abs_delta'] / df.loc[mask, 'target_val']

    # Sort: ENTER candidates by priority, others by abs_delta and symbol
    df['_sort_priority'] = df['symbol'].map(enter_priority_map)
    df['_is_enter'] = df['symbol'].isin(enter_priority_map.keys())
    df = df.sort_values(
        by=['_is_enter', '_sort_priority', 'abs_delta', 'symbol'],
        ascending=[False, True, False, True],
        na_position='last'
    )
    df = df.drop(columns=['_is_enter', '_sort_priority'])

    # --- DRIFT REBALANCE ---

    #Firstly, only do rebalancing if we had a change in position candidates on the day.
    if any(c.action in ('ENTER', 'EXIT') for c in candidates):

        df_drift = df[
            (~df['symbol'].isin(candidate_map.keys())) &
            (df['drift_pct'] > DRIFT_THRESHOLD)
        ]

        if not df_drift.empty:

            df_drift = df_drift.sort_values('abs_delta', ascending=False)

            # SELL overweight if negative cash
            if local_cash < Decimal('0'):

                for _, row in df_drift[df_drift['delta'] > 0].iterrows():

                    if local_cash >= Decimal('0'):
                        break

                    price = to_price(row['price'])
                    deficit = abs(local_cash)
                    # 1. Calculate how much you are OVERWEIGHT (the surplus)
                    # delta = current_val - target_val. If delta > 0, it's the $ amount you can sell.
                    surplus_val = Decimal(str(row['delta']))

                    # 2. Convert that surplus $ into a maximum quantity you're allowed to sell
                    max_qty_to_rebalance = (surplus_val / price).quantize(Decimal('1'), rounding=ROUND_DOWN)

                    # 3. Determine how many units you NEED to sell to cover the cash deficit
                    qty_needed_for_cash = (deficit / price).quantize(Decimal('1'), rounding=ROUND_UP)

                    # 4. Final qty is the lesser of: what you NEED vs what you are ALLOWED to sell
                    qty = min(max_qty_to_rebalance, qty_needed_for_cash)

                    # 5. Safety check: never sell more than you actually own
                    held_qty = Decimal(str(row['qty_held'])).quantize(Decimal('1'), rounding=ROUND_DOWN)
                    qty = max(Decimal('0'), min(qty, held_qty))

                    local_cash = execute_trade(
                        account_id,
                        row['symbol'],
                        "SELL",
                        qty,
                        price,
                        eval_date,
                        local_cash,
                        trades,
                        cash_ledger_entries,
                        f"Drift rebalance SELL {qty} {row['symbol']}"
                    )

            # BUY underweight if excess cash
            elif local_cash > Decimal('1000'):

                for _, row in df_drift[df_drift['delta'] < 0].iterrows():

                    if local_cash <= Decimal('100'):
                        break

                    price = to_price(row['price'])
                    target_gap = Decimal(str(abs(row['delta'])))
                    
                    # 1. Max we can afford based on current cash
                    max_affordable = (local_cash - Decimal('1.00')) / Decimal('1.001')
                    
                    # 2. Our "Gross Target" is the smaller of what we need vs what we have
                    gross_target_value = min(target_gap, max_affordable)
                    
                    # 3. Apply your methodology
                    gross_qty = (gross_target_value / price).quantize(Decimal('1'), rounding=ROUND_DOWN)
                    commission = calculate_commission(gross_qty)
                    
                    net_target_value = gross_target_value - commission
                    
                    # 4. Final safety check: ensure net value is still positive
                    if net_target_value > 0:
                        qty = (net_target_value / price).quantize(Decimal('1'), rounding=ROUND_DOWN)
                        
                        if qty > 0:
                            local_cash = execute_trade(
                                account_id,
                                row['symbol'],
                                "BUY",
                                qty,
                                price,
                                eval_date,
                                local_cash,
                                trades,
                                cash_ledger_entries,
                                f"Drift rebalance BUY {qty} {row['symbol']}"
                            )

    # --- COMMIT ---
    for trade in trades:
        at.record_trade(trade)

    for ledger in cash_ledger_entries:
        acl.record_cash_transaction(ledger)

    return trades

def create_daily_snapshot(
    account_id: int, 
    eval_date: date, 
    previous_holdings: List[ah.AccountHolding], # Already in memory
    today_trades: List[at.AccountTrade]         # Returned from rebalance
) -> tuple[Decimal, List[ah.AccountHolding]] :
    
    # 1. Load data into DataFrames
    df_prev = pd.DataFrame([vars(h) for h in previous_holdings])
    if df_prev.empty:
        df_prev = pd.DataFrame(columns=['symbol', 'quantity', 'cost_basis'])
    
    # Convert to float immediately
    df_prev['quantity'] = df_prev['quantity'].astype(float)
    df_prev['cost_basis'] = df_prev['cost_basis'].astype(float)


    # --- APPLY SPLITS TO PREVIOUS HOLDINGS ---
    # We must adjust the "carried over" shares to match today's post-split reality
    # BEFORE we merge with today's trades.
    
    # # Fetch split factors for all currently held symbols on this specific date
    # # Returns a dict: {'AAPL': 4.0, 'TSLA': 3.0} for a 4:1 and 3:1 split
    # split_map = ticker_split_history.fetch_split_factors_on_date(df_prev['symbol'].tolist(), eval_date)
    
    # if split_map:
    #     def apply_split(row):
    #         symbol = row['symbol']
    #         factor = split_map.get(symbol, 1.0)
            
    #         if factor != 1.0:
    #             # Quantity increases (or decreases) by the factor
    #             # Total Cost Basis remains UNCHANGED (your investment value didn't change)
    #             return row['quantity'] * factor
    #         return row['quantity']

    #     df_prev['quantity'] = df_prev.apply(apply_split, axis=1)
        
    #     # Optional: Log the adjustment for debugging
    #     for sym, factor in split_map.items():
    #         print(f"Applied {factor}:1 split adjustment to {sym} holdings on {eval_date}")


    df_trades = pd.DataFrame([vars(t) for t in today_trades])

    # 2. Process Trades (Aggregate by Symbol)
    if not df_trades.empty:

        df_trades['quantity'] = df_trades['quantity'].astype(float)
        df_trades['total_amount'] = df_trades['total_amount'].astype(float)
        df_trades['commission'] = df_trades['commission'].astype(float)

        # Net Quantity: BUY is +, SELL is -
        df_trades['qty_delta'] = df_trades.apply(
            lambda x: float(x['quantity']) if x['side'] == 'BUY' else -float(x['quantity']), axis=1
        )
        # Cost Basis Delta: Only BUYS increase cost basis (Price + Fee)
        df_trades['cost_delta'] = df_trades.apply(
            lambda x: float(x['total_amount'] + x['commission']) if x['side'] == 'BUY' else 0.0, axis=1
        )
        # Track total quantity sold for pro-rata cost reduction
        df_trades['qty_sold'] = df_trades.apply(
            lambda x: float(x['quantity']) if x['side'] == 'SELL' else 0.0, axis=1
        )
        
        trade_summary = df_trades.groupby('symbol').agg({
            'qty_delta': 'sum',
            'cost_delta': 'sum',
            'qty_sold': 'sum'
        }).reset_index()
    else:
        trade_summary = pd.DataFrame(columns=['symbol', 'qty_delta', 'cost_delta', 'qty_sold'])

    # 3. Merge Yesterday and Today
    df = pd.merge(
        df_prev[['symbol', 'quantity', 'cost_basis']], 
        trade_summary, 
        on='symbol', 
        how='outer'
    )
    df = df.infer_objects(copy=False)
    num_cols = ['quantity', 'cost_basis', 'qty_delta', 'cost_delta', 'qty_sold']
    for col in num_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0.0).astype(float)
            
    # 4. Calculate New Quantity
    df['new_qty'] = df['quantity'] + df['qty_delta']
    
    # 5. Calculate Adjusted Cost Basis
    # For Sells: Reduce cost basis by the % of the position sold
    def calculate_new_cost(row):
        old_qty = float(row['quantity'])
        old_cost = float(row['cost_basis'])
        qty_sold = float(row['qty_sold'])
        
        if old_qty > 0 and qty_sold > 0:
            sell_pct = min(1.0, qty_sold / old_qty)
            old_cost = old_cost * (1.0 - sell_pct)
            
        return old_cost + row['cost_delta']

    df['new_cost'] = df.apply(calculate_new_cost, axis=1)
    
    # Filter for active holdings (remove positions closed today)
    df = df[df['new_qty'] > 1e-8].copy()
    if df.empty: 
        return Decimal(0.0), []

    # 6. Market Value & Weights
    price_map = {}
    for symbol in df['symbol'].unique():
        latest_date = ticker_value.fetch_latest_price_date_for_ticker(symbol, eval_date)
        if latest_date:
            tv = ticker_value.fetch_ticker_on_date(symbol, latest_date)
            if tv and tv.stock_price:
                price_map[symbol] = float(tv.stock_price)
    
    # Map latest available prices
    df['price'] = df['symbol'].map(price_map)

    # Fallback logic: If today's price is missing/zero, use previous day's implied price
    # We calculate implied_prev_price from the previous holdings we already have in df_prev
    if not df_prev.empty:
        # Avoid division by zero for symbols that had 0 qty yesterday
        df_prev['prev_price'] = df_prev.apply(
            lambda x: float(x['market_value']) / float(x['quantity']) if float(x['quantity']) > 0 else 0.0, axis=1
        )
        prev_price_map = dict(zip(df_prev['symbol'], df_prev['prev_price']))
        # Fill only the NaNs (where today's price was 0 or missing)
        df['price'] = df['price'].fillna(df['symbol'].map(prev_price_map))

    # Final fallback to 0.0 for entirely new symbols with no history and no current price
    df['price'] = df['price'].fillna(0.0)
    df['mkt_val'] = df['new_qty'] * df['price']

    # Cash at End of Day
    eod_cash = float(acl.get_cash_balance(account_id, eval_date + timedelta(days=1)))
    tpv = df['mkt_val'].sum() + eod_cash
    df['weight'] = df['mkt_val'] / tpv if tpv > 0 else 0.0

    snapshots = [
        ah.AccountHolding(
            account_id=account_id,
            holding_date=eval_date,
            symbol=row['symbol'],
            quantity=Decimal(str(round(row['new_qty'], 0))),
            cost_basis=Decimal(str(round(row['new_cost'], 2))),
            market_value=Decimal(str(round(row['mkt_val'], 2))),
            weight_percentage=Decimal(str(round(row['weight'], 6)))
        ) for _, row in df.iterrows()
    ]

    # 7. Record into database
    ah.record_account_holdings(snapshots)
    daily_return = ap.record_daily_performance(account_id=account_id, eval_date=eval_date, cash_balance=Decimal(round(eod_cash,2)), snapshots=snapshots)

    return daily_return, snapshots

def benchmark_comparison(account_id: int, fund_id: int, eval_date: date, daily_return: Decimal, snapshots: List[ah.AccountHolding]):
    fund_data = fund.fetch_fund(fund_id) # Using your existing fetch_fund

    if (fund_data is None):
        raise Exception("Account has not been assoicated a fund strategy")

    strategy = getStrategyFromJson(fund_data.strategy)
    
    if not strategy or not strategy.benchmarks:
        return

    for symbol in strategy.benchmarks:
        # Get Benchmark Prices
        today_bench = bv.fetch_benchmark_price(symbol, eval_date)
        prev_bench = bv.fetch_latest_benchmark_price_before(symbol, eval_date)

        if not today_bench or not prev_bench:
            continue

        # Calculate Benchmark Metrics
        bench_return = (today_bench.price / prev_bench.price) - 1
        alpha = daily_return - bench_return

        # Calculate Indexed Growth ($1.00 starting value)
        prev_strat_idx, prev_bench_idx = abc.fetch_previous_comparison_values(account_id, symbol, eval_date)
        
        new_strat_idx = (prev_strat_idx * (1 + daily_return)).quantize(Decimal('0.000001'))
        new_bench_idx = (prev_bench_idx * (1 + bench_return)).quantize(Decimal('0.000001'))

        # Save Comparison
        abc.record_benchmark_comparison(abc.AccountBenchmarkComparison(
            account_id=account_id,
            benchmark_symbol=symbol,
            performance_date=eval_date,
            strategy_indexed_value=new_strat_idx,
            benchmark_indexed_value=new_bench_idx,
            daily_alpha=alpha.quantize(Decimal('0.000001'))
        ))

def get_account_holdings(account_id: int, eval_date: date) -> list[ah.AccountHolding]:
    account_holdings = ah.fetch_current_account_snapshot(account_id, eval_date)

    # Verify price availability status for holdings for this date
    held_symbols = list([h.symbol for h in account_holdings])
    current_ticker_data = ticker_value.fetch_tickers_by_symbols_on_date(held_symbols, eval_date)
    symbols_with_prices = {t.symbol for t in current_ticker_data if t.stock_price}
    missing_prices = [s for s in held_symbols if s not in symbols_with_prices]
    if len(missing_prices) != 0 and len(missing_prices) != len(held_symbols):
        print(f"** Missing pricing on {eval_date} for {len(missing_prices)} out of {len(held_symbols)} - {', '.join(missing_prices)} - will use most recently available")

    return account_holdings

def daily_actions(account: account.Account, sim_date: date):
    if account.id is None:
        raise Exception('Account ID not specified')
    
    f = fund.fetch_fund(account.strategy_fund_id)
    if f is None:
        raise Exception("Missing strategy for fund")

    strategy = getStrategyFromJson(f.strategy)

    # Update Cash: Apply dividends 
    process_daily_dividends(account_id=account.id, eval_date=sim_date)   

    if sim_date.weekday() < 5: # Monday -> Friday
        account_holdings = get_account_holdings(account_id=account.id, eval_date=sim_date)

        # Check for changes in target fund_holdings
        fund_symbols, candidates = identify_position_change_needs(account_id=account.id, fund_id=account.strategy_fund_id, eval_date=sim_date, account_holdings=account_holdings)
        
        # Execute the changes and rebalance
        today_trades = execute_minimal_rebalance(account_id=account.id, num_strategy_holdings=strategy.holdings, candidates=candidates, eval_date=sim_date, account_holdings=account_holdings)

        # Create Today's Snapshot
        daily_return, snapshots = create_daily_snapshot(account_id=account.id, eval_date=sim_date, previous_holdings=account_holdings, today_trades=today_trades)

        # Record Performance
        benchmark_comparison(account_id=account.id, fund_id=account.strategy_fund_id, eval_date=sim_date, daily_return=daily_return, snapshots=snapshots)
 
    # Update interest on end of day cash 
    process_daily_interest(account_id=account.id, eval_date=sim_date)    
   




