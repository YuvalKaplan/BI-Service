import log
from typing import List
from datetime import date
from modules.core.fdata import get_etf_holdings
from modules.bt.object import provider_etf, provider_etf_holding


REMOVE_ETFS_AND_FUNDS = r'\b(ETF|fund)\b'

def run(etf_ids: List[int], today: date):
    try:
        for etf_id in etf_ids:
            holdings = provider_etf_holding.fetch_valid_holdings_by_provider_etf_id(etf_id, today)

            if len(holdings) != 0:
                return
            
            etf_details = provider_etf.fetch_by_id(etf_id)
            if etf_details is None:
                raise Exception(f"ETF details not found for ETF ID {etf_id}")

            if etf_details.ticker is None:
                    raise Exception(f"Holdings not found for provider ETF {etf_details.name} ({etf_details.id})")
            
            holdings: List[provider_etf_holding.ProviderEtfHolding] = []

            raw_holdings = get_etf_holdings(etf_details.ticker, today)
            if isinstance(raw_holdings, str):
                raise Exception(f"Holdings not downloaded from data provider for ETF {etf_details.name} ({etf_details.id})")
            else:
                for rh in raw_holdings:
                    if rh['assetType'] == 'Equity':
                        holdings.append(provider_etf_holding.ProviderEtfHolding(provider_etf_id=etf_id, trade_date=today, ticker=rh['symbol'], shares=int(rh['shares']), market_value=float(rh['value']), weight=float(rh['percent'])))

                provider_etf_holding.insert_holding_bulk(holdings)
    
    except Exception as e:
        log.record_error(f"Error in downloading ETF holdings: {e}")
        raise e