import pandas as pd
from typing import cast
from modules.bt.object.provider_etf import fetch_by_ticker
from modules.bt.object.provider_etf_holding import ProviderEtfHolding, insert_holding_bulk

def process_etf_excel_to_db(file_path: str):
    excel_file = pd.ExcelFile(file_path)
    
    for etf_ticker in excel_file.sheet_names:
        print(f"--- Processing ETF: {etf_ticker} ---")
        
        try:
            etf_record = fetch_by_ticker(str(etf_ticker))
            provider_etf_id = etf_record.id
        except Exception as e:
            print(f"Skipping sheet '{etf_ticker}': {e}")
            continue

        # Load sheet and unpivot (melt) columns into rows
        df = pd.read_excel(excel_file, sheet_name=etf_ticker)
        
        # 'Ticker' and 'Name' stay as columns; all date columns become rows in 'trade_date'
        df_long = df.melt(
            id_vars=['Ticker', 'Name'], 
            var_name='trade_date', 
            value_name='shares'
        )

        # Convert trade_date to actual date objects and filter out invalid rows
        df_long['trade_date'] = pd.to_datetime(df_long['trade_date'], errors='coerce')
        df_long = df_long.dropna(subset=['trade_date', 'shares', 'Ticker'])
        df_long = df_long[df_long['shares'] > 0]

        # Iterate through unique dates to avoid "No overloads for to_datetime"
        unique_dates = df_long['trade_date'].unique()

        for dt_val  in unique_dates:
            ts = cast(pd.Timestamp, dt_val )
            if pd.isna(ts):
                continue

            current_date = ts.date()

            # Filter the dataframe for this specific date
            day_group = df_long[df_long['trade_date'] == dt_val]

            holdings_to_insert = [
                ProviderEtfHolding(
                    provider_etf_id=provider_etf_id,
                    trade_date=current_date,
                    ticker=str(row['Ticker']).strip().upper(),
                    shares=float(row['shares']),
                    market_value=0.0,
                    weight=0.0
                )
                for _, row in day_group.iterrows()
            ]

            if holdings_to_insert:
                try:
                    insert_holding_bulk(holdings_to_insert)
                    # print(f"Success: {etf_ticker} | {current_date} | {len(holdings_to_insert)} rows")
                except Exception as e:
                    print(f"Failed: {etf_ticker} | {current_date}: {e}")

if __name__ == "__main__":
    process_etf_excel_to_db("etf_holdings.xlsx")
