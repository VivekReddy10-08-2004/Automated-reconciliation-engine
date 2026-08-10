import os 
from dotenv import load_dotenv 
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.requests import OptionBarsRequest, OptionChainRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

# Load environment variables from .env file
load_dotenv("project.env")

api_key = os.getenv("ALPACA_API_KEY")
secret_key = os.getenv("ALPACA_SECRET_KEY")

#Initializing the options client
client = OptionHistoricalDataClient(api_key, secret_key)

#Setting timezone awarenes dates
now = datetime.now()
start_date = now - timedelta(days=5) # we define start_date 5 days ago

request_params = OptionBarsRequest(
    symbol_or_symbols= "SPY241220C00500000",
    timeframe=TimeFrame(1, TimeFrameUnit.Hour), # 1 hour bars
    start= start_date,
    end= now
)
#Fetching the data
option_bars = client.get_option_bars(request_params)
options_chain_request = OptionChainRequest(
    underlying_symbol="SPY",
    expiration_dates=[datetime(2026, 12, 20)]
)

#Converting the data to a DataFrame
option_df_bars = option_bars.df

if __name__ == "__main__":
    print(option_df_bars)
    chain_dict = client.get_option_chain(options_chain_request)
    print(chain_dict)

