import os
import sqlite3

from tqdm import tqdm
import utile_func as uf


###
# reference
# 1. https://github.com/binance/binance-public-data/
# # - All intervals are supported(freq): 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1mo
# # # - 1mo is used instead of 1M to supprt non-case sensitive file systems.
# # - example : https://data.binance.vision/data/spot/monthly/klines/BNBUSDT/1m/BNBUSDT-1m-2019-01.zip
# # - symbol list : https://data.binance.vision

# 2. https://coinmarketcap.com/ko/
# # - order by : cap
###


class BinanceKline:
    def __init__(self, freq, limit, download=True, sql=False, csv=False, remove=True):
        self.CoinMarketCap = uf.CoinMarketCap(limit)
        self.freq = freq
        self.dir_coins = f"data/coins_{freq}"
        self.dir_merge = f"data/merge_{freq}"
        self.download = download
        self.csv = (csv ^ sql) & csv
        self.sql = not csv
        self.remove = remove

    def data_call(self):
        # dir create - merge data
        uf.CreateDirectory(self.dir_merge)

        symbols = list(self.CoinMarketCap["symbol"])
        exclusive_list = []
        extension = ""
        conn = None
        if self.csv:
            extension = ".csv"
            exclusive_list = os.listdir(self.dir_merge)
        elif self.sql:
            conn = sqlite3.connect(f"data/coin_{self.freq}.db")

            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

            exclusive_sql_list = cursor.fetchall()
            exclusive_list = [table[0] for table in exclusive_sql_list]

        for symbol in tqdm(symbols):

            if f"{symbol}_{self.freq}{extension}" not in exclusive_list:
                # dir create : coins
                uf.CreateDirectory(f"{self.dir_coins}/{symbol}")

                # 바이낸스 데이터 가져오기.
                if self.download:
                    uf.binance_data_call(self.dir_coins, symbol, self.freq)

                # 데이터 병합.
                df = uf.data_merge(self.dir_coins, symbol)

                if self.csv:
                    df.to_csv(f"{self.dir_merge}/{symbol}_{self.freq}.csv")

                if self.sql:
                    df.to_sql(f"{symbol}_{self.freq}", conn, index="Open time", chunksize=1000, if_exists="append")

                # dir remove : coins ~/ symbol
                if self.remove:
                    uf.RemoveDirectory(f"{self.dir_coins}/{symbol}")


if __name__ == '__main__':
    BK = BinanceKline(freq="1m", limit=100, csv=True)
    BK.data_call()
