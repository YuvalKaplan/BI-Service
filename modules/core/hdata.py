import log as log
import os
from datetime import date
import pandas as pd
from fds.sdk.utils.authentication import ConfidentialClient
from fds.sdk.FactSetOwnership import ApiClient, Configuration
from fds.sdk.FactSetOwnership.api import fund_holdings_api
from fds.sdk.FactSetOwnership.models import FundHoldingsRequest, IdFundHoldings, AssetType

USER = "INSIGHT_IL-2324063"
API_KEY = os.getenv('SECRET_HOLDINGS_DATA_API_KEY')

def get_etf_holdings(symbol: str, as_of_date: date) -> pd.DataFrame:
    config = Configuration(username=USER, password=API_KEY)

    with ApiClient(config) as api_client:
        api_instance = fund_holdings_api.FundHoldingsApi(api_client)
        
        try:
            response = api_instance.get_ownership_holdings(
                ids=[f"{symbol}-US"],   # IMPORTANT: FactSet format
                date=as_of_date.strftime("%Y-%m-%d"),
                asset_type="EQ"
            )

            if not response.data:
                return pd.DataFrame()

            return pd.DataFrame([item.to_dict() for item in response.data])

        except Exception as e:
            print(f"Error fetching {symbol} @ {as_of_date}: {e}")
            return pd.DataFrame()