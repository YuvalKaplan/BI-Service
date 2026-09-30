import log
from datetime import date, datetime
from modules.core import api_stocks
from modules.bt.object import categorize_etf, categorize_etf_holding, categorize_ticker
from modules.bt.object.categorize_ticker import CategorizeTicker
from modules.bt.calc import classification


def _holdings_date(rows: list[dict]) -> date:
    """FMP's date for a fund's holdings (their latest updatedAt), else today."""
    stamps = [r.get('updatedAt') for r in rows if r.get('updatedAt')]
    try:
        return datetime.fromisoformat(max(stamps)).date() if stamps else date.today()
    except ValueError:
        return date.today()


def download_data() -> None:
    """Read the style reference ETFs' holdings from FMP (by ticker) and populate the BT categorize_ticker table with style and factors."""
    log.record_status("BT: Starting categorize ETF download.")

    # --- Style (Growth/Value) ETFs → BT categorize_etf_holding + categorize_ticker ---
    style_etfs = categorize_etf.fetch_all('style')
    log.record_status(f"BT: Processing {len(style_etfs)} style ETFs.")

    for etf in style_etfs:
        if not (etf.id and etf.ticker):
            continue
        log.record_status(f"BT: Processing '{etf.name}' ({etf.ticker}) for categorization.")
        rows = api_stocks.get_etf_holdings(etf.ticker)
        symbols = list(dict.fromkeys(r['asset'] for r in rows if r.get('asset') and (r.get('weightPercentage') or 0) > 0))
        if not symbols:
            log.record_notice(f"BT: No FMP holdings for style ETF '{etf.name}' ({etf.ticker}).")
            continue
        categorize_etf_holding.insert_holding(etf.id, _holdings_date(rows), symbols)
        categorize_etf.update_last_download(etf.id)
        rows = [
            CategorizeTicker(
                symbol=s,
                style_type=etf.style_type,
                cap_type=etf.cap_type,
                sector="Unknown",
                market_cap=0,
                esg_qualified=None,
                factors={},
            )
            for s in symbols
        ]
        categorize_ticker.upsert_bulk(rows)

    # --- Fetch factors for all BT categorize_ticker symbols ---
    ct_symbols = categorize_ticker.fetch_symbols()
    log.record_status(f"BT: Fetching factors for {len(ct_symbols)} categorize_ticker symbols.")
    factor_updates = classification.update_factor_cache(ct_symbols)
    categorize_ticker.bulk_update_factors(factor_updates)
    log.record_status(f"BT: Factor cache updated for {len(factor_updates)} symbols.")

    log.record_status("BT: Finished categorize ETF download.")
