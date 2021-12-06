import os
import io
import shutil
import zipfile

from datetime import datetime
from dateutil.rrule import rrule, MONTHLY

import requests
import threading
import pandas as pd

pool_sema = threading.Semaphore(10)  # 카운터가 10인 세마포어 생성


def months(start_month, start_year, end_month, end_year) -> list:
    start = datetime(start_year, start_month, 1)
    end = datetime(end_year, end_month, 1)
    return [(d.month, d.year) for d in rrule(MONTHLY, dtstart=start, until=end)]


def CoinMarketCap(limit) -> pd.DataFrame:
    url = f"https://api.coinmarketcap.com/data-api/v3/cryptocurrency/" \
          f"listing?start=1&limit={limit}&sortBy=market_cap&" \
          f"sortType=desc&cryptoType=all&tagType=all&audited=false" \
          f"&aux=num_market_pairs,cmc_rank"
    r = requests.get(url).json()
    df = pd.DataFrame(columns=["id", "symbol", "cmcRank", "isActive", "q_name", "q_price", "q_marketCap"])

    for dt in r['data']['cryptoCurrencyList']:
        in_data = {
            "id": dt["id"],
            "symbol": dt["symbol"],
            "cmcRank": dt["cmcRank"],
            "isActive": dt["isActive"],
            "q_name": dt["quotes"][0]["name"],
            "q_price": dt["quotes"][0]["price"],
            "q_marketCap": dt["quotes"][0]["marketCap"]
        }
        df = df.append(in_data, ignore_index=True)

    return df


def CreateDirectory(file_dir):
    directory = f"{file_dir}"

    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print('Error: Creating directory. ' + directory)


def RemoveDirectory(file_dir):
    try:
        if os.path.exists(file_dir):
            shutil.rmtree(file_dir)
    except OSError:
        print('Error: Remove directory. ' + file_dir)


def binance_data_call(dir_coins, symbol, freq, start_month=8, start_year=2017, end_month=11, end_year=2021):
    _base = "USDT"

    year_month = months(start_month, start_year, end_month, end_year)

    # Data
    data_thread_list = [
        threading.Thread(target=execution_data_call, args=(month, year, symbol, _base, freq, dir_coins),
                         daemon=True)
        for
        month, year in year_month]

    for idx, thread in enumerate(data_thread_list):
        thread.start()

    for idx, thread in enumerate(data_thread_list):
        thread.join()


def execution_data_call(month, year, symbol, _base, freq, dir_coins):
    # 세마포어 acquire
    pool_sema.acquire()

    try:
        month = str(month).zfill(2)
        url_date = f"{year}-{month}"
        url = f"https://data.binance.vision/data/spot/monthly/klines/" \
              f"{symbol}{_base}/{freq}/{symbol}{_base}-{freq}-{url_date}.zip"
        r = requests.get(url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(f"{dir_coins}/{symbol}")
    except Exception as e:
        # print(e)
        pass

    # 세마포어 release
    pool_sema.release()


def data_merge(dir_coins, symbol) -> pd.DataFrame:
    path_path_coins = f"{dir_coins}/{symbol}"

    file_list = os.listdir(path_path_coins)

    file_list = sorted(file_list)

    dfs = pd.DataFrame(columns=["Open time", "Open", "High", "Low", "Close", "Volumn", "Close time",
                                "Quote asset volume", "Number of trades", "Taker buy base asset volume",
                                "Taker buy quote asset volume", "Ignore"])
    dfs = dfs.set_index("Open time")

    for file_name in file_list:
        df = pd.read_csv(f"{path_path_coins}/{file_name}",
                         names=["Open time", "Open", "High", "Low", "Close", "Volumn", "Close time",
                                "Quote asset volume", "Number of trades", "Taker buy base asset volume",
                                "Taker buy quote asset volume", "Ignore"], header=None)
        df = df.set_index("Open time")
        dfs = pd.concat([dfs, df], axis=0)
    dfs.index = pd.to_datetime(dfs.index, unit="ms")

    return dfs
