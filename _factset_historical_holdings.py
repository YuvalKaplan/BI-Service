import pandas as pd
from time import sleep
from typing import cast
from datetime import date, datetime
from modules.bt.object.provider_etf import fetch_by_ticker
from modules.bt.object.provider_etf_holding_factset import ProviderEtfHoldingFactSet, insert_holding_bulk
from modules.core.hdata import get_etf_holdings, get_etf_holdings_via_formula

etf_symbols = ["GSGO", "DVAL", "GUSE", "GDIV", "FFTY", "PSET", "PY", "EQIN", "HUSV", "INCE", "GVIP", "LRGE", "USMC", "BOUT", "MMLG", "DIVZ", "ATFV", "PGRO", "JAVA", "WINN", "CGUS", "CGGR", "CGDV", "DUHP", "NBDS", "JGRO", "HAPI", "HIDV", "LOWV", "TOLL", "ECML", "BLCV", "TCAF", "TGRT", "TVAL", "BCHP", "JPEF", "SMRI", "LRGC", "BLCR"]
etf_symbols = ["DIVZ"]
start_date = date(2022,1,1)
end_date = date(2026,1,15)

def get_provider_etf_id(symbol: str) -> int:
    try:
        etf = fetch_by_ticker(symbol)
        return etf.id
    except Exception as e:
        print(f"ETF not found for ticker {symbol}: {e}")
        return 0
    
def generate_dates(start_date: date, end_date: date, step_days: int = 4):
    return pd.date_range(start=start_date, end=end_date, freq=f"{step_days}D").date


def map_df_to_db_items(df: pd.DataFrame, provider_etf_id: int, trade_date: date):
    if df.empty:
        return []

    items = []

    for _, row in df.iterrows():
        raw_ticker = row.get("security_ticker")
        shares = row.get("adj_holding")
        raw_date = row.get("date")
        if not raw_ticker or not shares or not raw_date:
            continue
        
        ticker = raw_ticker.split("-")[0]
        value_date = date.fromisoformat(raw_date)

        items.append(
            ProviderEtfHoldingFactSet(
                provider_etf_id=provider_etf_id,
                trade_date=trade_date,
                ticker=ticker,
                shares=shares,
                market_value=row.get("adj_market_value"),
                weight=row.get("weight_close"),
                created_at=value_date
            )
        )

    return items

def get_holdings():
    dates = generate_dates(start_date, end_date, step_days=4)

    for symbol in etf_symbols:
        provider_etf_id = get_provider_etf_id(symbol)

        if provider_etf_id == 0:
            continue

        print(f"\nProcessing ETF: {symbol} (ID={provider_etf_id})")

        for d in dates:
            print(f"  Fetching date: {d}")

            df = get_etf_holdings_via_formula(symbol, d)

            if df.empty:
                print("    No data returned")
                continue

            print(df)

            # items = map_df_to_db_items(df, provider_etf_id, d)

            # if not items:
            #     print("    No valid holdings after mapping")
            #     continue

            # try:
            #     insert_holding_bulk(items)
            #     print(f"    Inserted {len(items)} rows")

            #     # FactSet throttling
            #     sleep(0.25)

            # except Exception as e:
            #     print(f"    DB insert error: {e}")
    
    

if __name__ == "__main__":
    get_holdings()
