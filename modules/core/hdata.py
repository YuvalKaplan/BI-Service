import log as log
import os
from datetime import date
import pandas as pd
# from fds.sdk.FactSetOwnership import ApiClient, Configuration
import fds.sdk.Formula
from fds.sdk.Formula.apis import TimeSeriesApi
from fds.sdk.Formula.models import TimeSeriesRequest, TimeSeriesRequestData
from fds.sdk.FactSetOwnership.api import fund_holdings_api

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
        
def get_etf_holdings_via_formula(symbol: str, as_of_date: date) -> pd.DataFrame:
    try:
        config = fds.sdk.Formula.Configuration(username=USER, password=API_KEY)
        with fds.sdk.Formula.ApiClient(config) as api_client:           
            # Create Instance
            api_instance = TimeSeriesApi(api_client)
            # Request Object to Define Parameters
            time_series_request = TimeSeriesRequest(
                data=TimeSeriesRequestData(
                    formulas = [
                        "P_PRICE(0)",
                    ],
                    flatten = "Y",
                    universe = f"({symbol}-US)=1",
                ),
            )

            # Send Request
            api_response_wrapper = api_instance.get_time_series_data_for_list(
                time_series_request
            )
            api_response = api_response_wrapper.get_response_200()

            # Convert to Pandas Dataframe
            results = pd.DataFrame(api_response.to_dict()["data"])
            return results

    except Exception as e:
        print(f"Error fetching {symbol} @ {as_of_date}: {e}")
        return pd.DataFrame()