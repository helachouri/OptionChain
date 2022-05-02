import os
from datetime import timedelta, date

from pandas import DataFrame, DatetimeIndex, read_csv
from requests import get


def next_third_friday(d: date) -> date:
    """
    Given a third friday find next third friday
    """
    d += timedelta(weeks=4)
    return d if d.day >= 15 else d + timedelta(weeks=1)


class OptionChain:
    def __init__(self, api_token: str, symbol: str, dump_path: str = r"Opt_Chain/"):
        self.headers = {"Accept": "application/json", "Authorization": f"Bearer {api_token}"}
        self.dump_path = dump_path
        self.symbol = symbol
        self.cache = {}

    def request(self, endpoint: str):
        response = get(url=f"https://api.tradier.com/{endpoint}", headers=self.headers)
        if response.status_code == 200:
            return response.json()

        print(f"Error code : {response.status_code}")

    def historical_data(self):
        return self.request(f"/v1/markets/history?symbol={self.symbol}")

    def load_data(self, symbol: str, year: int) -> DataFrame:
        path = self.dump_path + f"{year}/{symbol}.csv"
        if os.path.exists(path):
            df = read_csv(path)
            df = df.set_index(DatetimeIndex(df["date"]))
            df = df.loc[:, ~df.columns.str.contains("date")]
            df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
        else:
            if not os.path.exists(path.split(symbol)[0]):
                os.mkdir(path.split(symbol)[0])

            df = DataFrame(self.historical_data().get("history", {}).get("day", []))
            df = df.set_index(DatetimeIndex(df["date"]))
            df = df.loc[:, ~df.columns.str.contains("date")]
            df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
            df.to_csv(path)

        return df

    def option(self, expiration: date, strike: int, option_type: str = "C") -> DataFrame:
        chain = f"{self.symbol}{expiration.strftime('%y%m%d')}{option_type}000{strike}000"

        if chain in self.cache:
            return self.cache[chain]

        df = self.load_data(chain, expiration.year)
        self.cache[chain] = df

        return df

    def download(self, year: int, strike_range: float = 0.1):
        # historical price data for the underlying
        data = self.load_data(self.symbol, year)

        # Determine monthly high and low for the underlying
        monthly_range = {}
        for month in range(1, 13):
            try:
                x = data[date(year, month, 1):date(year, month + 1, 1)]
                monthly_range[month] = {"low": min(x["low"]), "high": max(x["high"])}
            except KeyError:
                # If data for this month is unavailable, just extrapolate from the previous month
                monthly_range[month] = {"low": monthly_range[month - 1]["low"],
                                        "high": monthly_range[month - 1]["high"]}

        for month, prices in monthly_range.items():
            expiration = next_third_friday(date(year, month, 1))

            # Get all strikes that are strike_range% below the monthly low and strike_range% above the monthly high
            strikes = [x for x in
                       range(int(prices["low"] * (1 - strike_range)), int(prices["high"] * (1 + strike_range)))]

            # Download and save all of the option chains
            for strike in strikes:
                self.option(expiration, strike, "C")
                self.option(expiration, strike, "P")


if __name__ == '__main__':
    token = "<Your API token here>"
    ticker = "SPY"
    strike_range = 0.1
    api = OptChain(token, ticker, strike_range)
    api.download(2022)
