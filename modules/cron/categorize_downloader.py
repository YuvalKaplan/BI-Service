"""
The style reference ETFs (categorize_etf, usage 'style': a growth and a value ETF per cap size),
read from FMP by ticker (etf/holdings), weekly on Sunday. Each stock line is resolved into
categorize_ticker with its ETF's style and cap and its FMP factors
(TickerResolver.POPULATE_CATEGORY_TICKER) - the value / growth classifier's training set
(modules/calc/classification.py) and the CAT_ETF step of style assignment (modules/ticker/style.py).
"""
import log
from modules.core import api_stocks
from modules.object import batch_run, categorize_etf, categorize_etf_holding
from modules.sec import etf_profile
from modules.ticker import index_funds
from modules.ticker.resolver import TickerResolver


def run() -> int:
    """Stores each style ETF's FMP holdings in categorize_ticker; returns how many ETFs were
    stored. Raises when none of them has holdings on FMP."""
    try:
        batch_run_id = batch_run.insert(batch_run.BatchRun(process='categorize_download', activation='auto'))

        style_etfs = categorize_etf.fetch_all('style')
        log.record_status(f"Style (Growth/Value) update: processing {len(style_etfs)} style ETFs.")

        resolver = TickerResolver(TickerResolver.POPULATE_CATEGORY_TICKER)
        stored = 0
        for etf in style_etfs:
            if not (etf.id and etf.ticker and etf.style_type and etf.cap_type):
                log.record_notice(f"Style ETF '{etf.name}' has no ticker, style or cap - skipped.")
                continue
            rows = api_stocks.get_etf_holdings(etf.ticker)
            holding_date = index_funds.holdings_date(rows)
            if not rows or holding_date is None:
                log.record_notice(f"No FMP holdings for style ETF '{etf.name}' ({etf.ticker}).")
                continue

            log.record_status(f"Resolving the holdings of style ETF '{etf.name}' ({etf.ticker}, {holding_date})...")
            resolver.set_classification(etf.style_type, etf.cap_type)
            stocks = [r for r in rows if (r.get('weightPercentage') or 0) > 0 and etf_profile.is_stock_line(r, None)]
            cat_ticker_ids = []
            for r in stocks:
                try:
                    cat_ticker_id = resolver.resolve_fmp_line(r.get('asset') or None, r.get('isin') or None,
                                                              r.get('securityCusip') or None, r.get('name') or None)
                except Exception as e:
                    log.record_notice(f"Could not resolve '{r.get('asset') or r.get('name')}' of style ETF {etf.ticker}: {e}")
                    cat_ticker_id = None
                if cat_ticker_id is not None:
                    cat_ticker_ids.append(cat_ticker_id)

            categorize_etf_holding.insert_holding(etf.id, holding_date, cat_ticker_ids)
            categorize_etf.update_last_download(etf.id)
            stored += 1
            log.record_status(f"ETF '{etf.name}': {len(cat_ticker_ids)}/{len(stocks)} holdings resolved into categorize_ticker.")

        if style_etfs and stored == 0:
            raise Exception("None of the style ETFs has holdings on FMP")
        batch_run.update_completed_at(batch_run_id)
        log.record_status(f"Finished categorize ETF update.\n")
        return stored

    except Exception as e:
        log.record_error(f"Error in categorize ETF update: {e}")
        raise e
