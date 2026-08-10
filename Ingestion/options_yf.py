from datetime import datetime, timezone
import yfinance as yf
import pandas as pd

#The output we want to see
options_columns = [
    "underlying", 
    "expiry", 
    "strike", 
    "option_type", 
    "contract_symbol",
    "trade_date", 
    "close", 
    "bid",
    "ask",
    "volume", 
    "open_interest"
]

def fetch_option_chain(underlying: str, expiration: str) -> pd.DataFrame:

    #1
    chains = yf.Ticker(underlying).option_chain(expiration)
    calls = chains.calls
    puts = chains.puts

    calls["option_type"] = "C"
    puts["option_type"] = "P"

    combined = pd.concat([calls, puts], ignore_index=True)

    combined.rename(columns={
        "contractSymbol": "contract_symbol",
        "lastPrice": "close",
        "openInterest": "open_interest"
    }, inplace=True)

    combined["underlying"] = underlying
    combined["expiry"] = expiration
    combined["trade_date"] = datetime.now(timezone.utc).date()

    #Handling the case where there are no options available for the given underlying and expiration
    if calls.empty and puts.empty:
        return pd.DataFrame(columns=options_columns)
    return combined[options_columns]

def validate_columns(df: pd.DataFrame, expected_columns: list[str]) -> bool:
    """
    Validate that the DataFrame has the expected columns.
    """
    return list(df.columns) == expected_columns

if __name__ == "__main__":
    ticker = yf.Ticker("SPY")
    expiration_dates = ticker.options
    

    if available_expirations := expiration_dates:
        expiration = available_expirations[0]
        option_chain_df = fetch_option_chain("SPY", expiration)
        print(option_chain_df.head())
        print(validate_columns(option_chain_df, options_columns))