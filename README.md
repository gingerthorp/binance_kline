## Binance Kline Data 

> by seunghan(gingerthorp)

### reference
1. https://github.com/binance/binance-public-data/
   - All intervals are supported: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1mo  
   - 1mo is used instead of 1M to supprt non-case sensitive file systems.  
   - example : https://data.binance.vision/data/spot/monthly/klines/BNBUSDT/1m/BNBUSDT-1m-2019-01.zip  
   - symbol list : https://data.binance.vision  

2. https://coinmarketcap.com/ko/
   - order by : market_cap

### How to use(simple)
```
pip install requirements.txt

BK = BinanceKline(freq="1m", limit=100, sql=True)
BK = BinanceKline(freq="1m", limit=100, csv=True)
BK.data_call()
```