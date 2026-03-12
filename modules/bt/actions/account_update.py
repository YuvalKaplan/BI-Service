import pandas as pd
from decimal import Decimal, ROUND_DOWN
from typing import List, Tuple
from decimal import Decimal
from dataclasses import dataclass
from datetime import date, timedelta
from modules.bt.object import fund, fund_holding
from modules.bt.object import account, account_holding as ah, account_cash_ledger as acl, account_trade as at, account_performance as ap, account_benchmark_comparison as abc
from modules.bt.object import benchmark_value as bv
from modules.bt.object import interest_config, ticker_value, ticker_dividend_history


def process_daily_cash_accruals(account_id: int, sim_date: date):
    # 1. Interest
    balance = acl.get_cash_balance(account_id, sim_date)
    if balance > 0:
        rate_cfg = interest_config.get_latest_interest_rate(sim_date)
        if rate_cfg:
            daily_int = (balance * (rate_cfg.annual_rate / Decimal('100') / Decimal('365'))).quantize(Decimal('0.01'))
            acl.record_cash_transaction(acl.AccountCashLedger(
                account_id=account_id,
                transaction_date=sim_date,
                amount=daily_int,
                entry_type='INTEREST',
                description=f"Interest on {balance:,.2f}"
            ))

    # 2. Dividends
    divs = ticker_dividend_history.fetch_dividends_for_holdings(account_id, sim_date)
    for d in divs:
        total = (d['quantity'] * d['amount_per_share']).quantize(Decimal('0.01'))
        acl.record_cash_transaction(acl.AccountCashLedger(
            account_id=account_id,
            transaction_date=sim_date,
            amount=total,
            entry_type='DIVIDEND',
            description=f"Div: {d['symbol']} ({d['quantity']} shares)"
        ))

@dataclass
class TradeCandidate:
    symbol: str
    action: str  # 'ENTER', 'EXIT', 'DRIFT'
    priority: int
    current_qty: Decimal = Decimal('0')

def identify_rebalance_needs(account_id: int, fund_id: int, eval_date: date) -> Tuple[List[str], List[ah.AccountHolding], List[TradeCandidate]]:
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
        account_holdings = ah.fetch_current_account_snapshot(account_id=account_id, eval_date=eval_date)
        current_holdings = {h.symbol: h for h in account_holdings}

        all_symbols = set(targets_holdings.keys()) | set(current_holdings.keys())
        trade_candidates = []

        for sym in all_symbols:
            target = targets_holdings.get(sym)
            actual = current_holdings.get(sym)

            if target and not actual:
                trade_candidates.append(TradeCandidate(
                    symbol=sym, action="ENTER", priority=target.ranking
                ))
            elif actual and not target:
                trade_candidates.append(TradeCandidate(
                    symbol=sym, action="EXIT", priority=0, 
                    current_qty=Decimal(str(actual.quantity))
                ))
            elif actual and target:
                # Optional: Logic for drift rebalance can be added here
                pass 

        return target_symbols, account_holdings, trade_candidates

    except Exception as e:
        print(f"Error comparing holdings: {e}")
        return [], [], []

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

    if qty <= 0:
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
    num_fund_holdings: int,
    candidates: List[TradeCandidate],
    current_sim_date: date
) -> List[at.AccountTrade]:

    if not candidates:
        return []

    # --- FETCH DATA ---
    cash_val = float(acl.get_cash_balance(account_id, current_sim_date))
    current_holdings = ah.fetch_current_account_snapshot(account_id, current_sim_date)

    all_syms = list(set([h.symbol for h in current_holdings] + [c.symbol for c in candidates]))

    ticker_data = ticker_value.fetch_tickers_by_symbols_on_date(all_syms, current_sim_date)
    prices = {t.symbol: float(t.stock_price) for t in ticker_data if t.stock_price}

    holdings_map = {h.symbol: float(h.quantity) for h in current_holdings}

    # --- BUILD DATAFRAME ---
    df = pd.DataFrame({'symbol': all_syms})

    df['qty_held'] = df['symbol'].map(holdings_map).fillna(0.0)
    df['price'] = df['symbol'].map(prices).fillna(0.0)
    df['current_val'] = df['qty_held'] * df['price']

    tpv = cash_val + df['current_val'].sum()
    target_val_per_stock = tpv / num_fund_holdings if num_fund_holdings > 0 else 0.0

    # --- TARGET LOGIC ---
    candidate_map = {c.symbol: c.action for c in candidates}
    sell_symbols = [c.symbol for c in candidates if c.action == "EXIT"]
    buy_symbols = [c.symbol for c in candidates if c.action == "ENTER"]

    held_symbols = set(holdings_map)

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
    df = df.sort_values('abs_delta', ascending=False)

    trades: List[at.AccountTrade] = []
    cash_ledger_entries: List[acl.AccountCashLedger] = []

    local_cash = Decimal(str(cash_val)).quantize(Decimal('0.01'))

    # --- SELLS FIRST ---
    for _, row in df[df['symbol'].isin(sell_symbols)].iterrows():

        price = to_price(row['price'])

        delta_val = Decimal(str(row['delta']))
        qty_est = delta_val / price
        qty = qty_est.quantize(Decimal('1'), rounding=ROUND_DOWN)
        held_qty = Decimal(str(row['qty_held'])).quantize(Decimal('1'), rounding=ROUND_DOWN)
        qty = min(qty, held_qty)

        local_cash = execute_trade(
            account_id,
            row['symbol'],
            "SELL",
            qty,
            price,
            current_sim_date,
            local_cash,
            trades,
            cash_ledger_entries,
            f"Sold {qty} units of {row['symbol']}"
        )

    # --- BUYS SECOND ---
    for _, row in df[df['symbol'].isin(buy_symbols)].iterrows():

        if local_cash <= Decimal('2.00'):
            break

        price = to_price(row['price'])

        target_gross = Decimal(str(abs(row['delta'])))
        max_gross = (local_cash - Decimal('1.00')) / Decimal('1.001')
        actual_gross = min(target_gross, max_gross)
        qty_est = actual_gross / price
        qty = qty_est.quantize(Decimal('1'), rounding=ROUND_DOWN)

        local_cash = execute_trade(
            account_id,
            row['symbol'],
            "BUY",
            qty,
            price,
            current_sim_date,
            local_cash,
            trades,
            cash_ledger_entries,
            f"Bought {qty} units of {row['symbol']}"
        )

    # --- DRIFT REBALANCE ---
    if any(c.action in ('ENTER', 'EXIT') for c in candidates):

        df_drift = df[~df['symbol'].isin(candidate_map.keys())]

        if not df_drift.empty:

            df_drift = df_drift.sort_values('abs_delta', ascending=False)

            # SELL overweight if negative cash
            if local_cash < Decimal('0'):

                for _, row in df_drift[df_drift['delta'] > 0].iterrows():

                    if local_cash >= Decimal('0'):
                        break

                    price = to_price(row['price'])
                    deficit = abs(local_cash)
                    qty = (deficit / price).quantize(Decimal('1'), rounding=ROUND_DOWN)
                    held_qty = Decimal(str(row['qty_held'])).quantize(Decimal('1'), rounding=ROUND_DOWN)
                    qty = min(qty, held_qty)

                    local_cash = execute_trade(
                        account_id,
                        row['symbol'],
                        "SELL",
                        qty,
                        price,
                        current_sim_date,
                        local_cash,
                        trades,
                        cash_ledger_entries,
                        f"Drift rebalance SELL {qty} {row['symbol']}"
                    )

            # BUY underweight if excess cash
            elif local_cash > Decimal('20'):

                for _, row in df_drift[df_drift['delta'] < 0].iterrows():

                    if local_cash <= Decimal('5'):
                        break

                    price = to_price(row['price'])
                    target_gap = Decimal(str(abs(row['delta'])))
                    max_affordable = (local_cash - Decimal('1.00')) / Decimal('1.001')
                    buy_value = min(target_gap, max_affordable)
                    qty = (buy_value / price).quantize(Decimal('1'), rounding=ROUND_DOWN)

                    local_cash = execute_trade(
                        account_id,
                        row['symbol'],
                        "BUY",
                        qty,
                        price,
                        current_sim_date,
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
    
    df_trades = pd.DataFrame([vars(t) for t in today_trades])

    # 2. Process Trades (Aggregate by Symbol)
    if not df_trades.empty:
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
    ).fillna(0.0)
    
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
            
        return old_cost + float(row['cost_delta'])

    df['new_cost'] = df.apply(calculate_new_cost, axis=1)
    
    # Filter for active holdings (remove positions closed today)
    df = df[df['new_qty'] > 1e-8].copy()
    if df.empty: return Decimal(0.0), []

    # 6. Market Value & Weights
    prices_raw = ticker_value.fetch_tickers_by_symbols_on_date(df['symbol'].tolist(), eval_date)
    price_map = {p.symbol: float(p.stock_price) for p in prices_raw if p.stock_price}
    
    df['price'] = df['symbol'].map(price_map).fillna(0.0)
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
            quantity=Decimal(str(row['new_qty'])),
            cost_basis=Decimal(str(row['new_cost'])),
            market_value=Decimal(str(row['mkt_val'])),
            weight_percentage=Decimal(str(row['weight']))
        ) for _, row in df.iterrows()
    ]

    # 7. Record into database
    ah.record_account_holdings(snapshots)
    daily_return = ap.record_daily_performance(account_id=account_id, eval_date=eval_date, cash_balance=Decimal(eod_cash), snapshots=snapshots)

    return daily_return, snapshots

def benchmark_comparison(account_id: int, fund_id: int, eval_date: date, daily_return: Decimal, snapshots: List[ah.AccountHolding]):
    fund_data = fund.fetch_fund(fund_id) # Using your existing fetch_fund

    if (fund_data is None):
        raise Exception("Account has not been assoicated a fund strategy")

    strategy = fund.getStrategyFromJson(fund_data.strategy)
    
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

def daily_actions(account: account.Account, current_sim_date: date):
    if account.id is None:
        raise Exception('Account ID not specified')

    # Update Cash: Apply interest and record dividends 
    process_daily_cash_accruals(account.id, current_sim_date)
    
    # Check for changes in target fund_holdings
    fund_symbols, account_holdings, needs = identify_rebalance_needs(account.id, account.strategy_fund_id, current_sim_date)
    
    # Execute the changes and rebalance
    today_trades = execute_minimal_rebalance(account.id, len(fund_symbols), needs, current_sim_date)

    # Create Today's Snapshot
    # daily_return, snapshots = create_daily_snapshot(account_id=account.id, eval_date=current_sim_date, previous_holdings=account_holdings, today_trades=today_trades)
    
    # Record Performance
    # benchmark_comparison(account.id, account.strategy_fund_id, current_sim_date, daily_return, snapshots)




