from pykrx import stock
import pandas as pd
import numpy as np
import random
import os
import time
import matplotlib.pyplot as plt
from strategies import MovingAverageStrategy

strategy = MovingAverageStrategy()
tickers = stock.get_index_portfolio_deposit_file("1028")

num = int(input("Enter a number: "))

if num == 1:

  result = []
  for ticker_1034 in tickers:
    df = stock.get_market_ohlcv_by_date('20230101', '20230814', ticker_1034)
    result.append(strategy.double_moving_average_yield(df, 9, 23))
    time.sleep(1)

  s = pd.Series(result, tickers)
  s = s.sort_values(ascending=False)
  print(s)

elif num == 2:
  tickers = [
    "123700",
    "007120",
    "053060",
    "009770",
    "035460",
    "192390",
    "032750",
    "010280",
    "056730",
    "046310",
    "122690",
    "024740",
    "053620",
    "017000",
    "018680",
    "043710",
    "039610",
    "050860",
    "021650",
    "310870"
  ]
  for ticker in tickers:
    result = []
    results = {}
    df = stock.get_market_ohlcv('20230101', '20230814', ticker)
    for x in range(3,20) :
      for y in range(21, 120):
        try:
          result = strategy.double_moving_average_yield(df, x, y)
          key = f"{x}-{y}"  # x와 y 값을 사용하여 key 생성
          results[key] = result
        except IndexError:
          continue

    s1 = pd.Series(results)
    s1 = s1.sort_values(ascending=False).head(10)
    print('--------------', ticker)
    print(s1)

elif num == 3:
  result = []
  for ticker_1034 in tickers:
    df = stock.get_market_ohlcv_by_date('20230101', '20230814', ticker_1034)
    result.append(strategy.single_moving_average_yield(df, 5))
    time.sleep(1)

  s = pd.Series(result, tickers)
  s = s.sort_values(ascending=False)
  print(s)

elif num == 4:
  result = []

  df = stock.get_market_ohlcv('20230101', '20230814', '007120')
  result = strategy.double_moving_average_yield(df, 3, 65)
  print(result)

elif num == 5:

  result = []
  for ticker_1034 in tickers:
    df = stock.get_market_ohlcv('20220101', '20230814', ticker_1034)
    result.append(strategy.double_moving_average_yield_with_bollinger(df, 5, 20))


  s = pd.Series(result, tickers)
  s = s.sort_values(ascending=False)
  print(s)

else:
  pass

"""
-------------- 053060
18-30    1.404334
-------------- 046310
17-54    1.329212
-------------- 122690
8-36     1.399680
-------------- 310870
13-23    1.266502
"""



