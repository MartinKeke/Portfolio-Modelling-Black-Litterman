import requests
import pandas as pd
import time

class CryptoDataDownloader:
    def __init__(self, tickers, vs_currency='usd'):
        """
        Initialize the downloader with a list of cryptocurrency tickers and a fiat currency.
        """
        self.tickers = tickers
        self.vs_currency = vs_currency
         

    def download_data(self, frequency='M'):
        """
        Download and aggregate prices and market cap data for the cryptocurrencies.

        :param frequency: Frequency of data aggregation ('D' for daily, 'W' for weekly, 'M' for monthly).
        """
        results = {}
        wait_time = 3  # Initial wait time

        for ticker in self.tickers:
            while True:
                time.sleep(wait_time)
                url = f'https://api.coingecko.com/api/v3/coins/{ticker}/market_chart?vs_currency={self.vs_currency}&days=max'
                response = requests.get(url)

                if response.status_code == 200:
                    data = response.json()

                    if 'prices' not in data or 'market_caps' not in data:
                        print(f"Data for {ticker} does not contain 'prices' or 'market_caps'.")
                        break

                    prices = self.aggregate_data(data['prices'], frequency)
                    market_caps = self.aggregate_data(data['market_caps'], frequency)

                    combined_data = pd.DataFrame({
                        f'{frequency} Average Price': prices['value'],
                        f'{frequency} Average Market Cap': market_caps['value']
                    })

                    results[ticker] = combined_data
                    break
                elif response.status_code == 429:
                    print(f"Rate limit hit for {ticker}. Waiting for {wait_time} seconds before retrying...")
                    wait_time *= 2
                else:
                    print(f"Failed to retrieve data for {ticker}. HTTP Status Code: {response.status_code}")
                    break

            wait_time = 3  # Reset wait time for the next ticker

        return results

    @staticmethod
    def aggregate_data(data, frequency):
        """
        Extract and aggregate data by the specified frequency.

        :param data: The data to be aggregated.
        :param frequency: The frequency for aggregation ('D', 'W', 'M').
        """
        df = pd.DataFrame(data, columns=['timestamp', 'value'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        aggregated_data = df.resample(frequency).mean()
        return aggregated_data

    
    
    