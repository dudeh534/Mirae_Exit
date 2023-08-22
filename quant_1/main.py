
"""
방향성
1. 초반에는 보조지표들로 1차적인 대상선별
2. 선별된 대상을 알림받고, 해당 주식을 매매해야하는 이유 찾기
3. 매매하여 수익을 올리는 행위가 지속되는 루틴 찾기
4. 그 루틴이 이어진다면 1차선별, 2차선별 하나씩 더 자동화 해나가기. 한번에 모든 것을 하려고 하지 말자
5. 추가로 퀀트 알고리즘 기법이나, 머신러닝 기법 등을 추가로 탑재하기
기술적분석 -> 기본적분석 -> 매매

8/13 quant_1.ipynb
1. 이동평균선을 종가가 상향돌파할 때 매수하는 로직으로 224일 이동평균까지 데이터 중 가장 수익률이 좋은 이동평균 구하기
2. TODO : 종목별로 가장 좋은 이동평균 구하기, 종가가 아닌 이평선, 이평선으로 구해보기 -> 어느 시점에 알림을 보낼 것인지.
3. MACD 를 이용해서 로직 강화
4. 더 좋은 로직 탐색

정리
1. 골든크로스, 데드크로스 방식으로 최적의 단기 이평선, 장기 이평선 골든크로스를 찾아보자는 생각 -> 종목별 수익률, N이평선별 수익률
-> 그걸 찾아도 미래에 그 이평선의 유효할 것이라는 담보를 할 수 있을까
-> 타계점 : train set, validation set, test set 으로 나눠서 검증해보는 방법
2. 골든크로스에 거래량을 더해서 이동평균의 2배 보다 큰 거래량이 터졌을때 매수하는 방법
3. backtrader 이용하는 방법
4. 퀀터스에 발췌한 피터린치 기법의 포트폴리오로 먼저 1차 대상 색출 이후 기술적분석으로 매매 타이밍을 선정하는 것.

결론 : 그래서 앞으로 무슨 어떻게 해야하지? 심층신경망을 이용한 방법?
앞으로도 유의미하게 좋은 수익률을 거둘 수 있는 전략을 도출해내는 것.

1. 전략탐색
2. 전략구현
3. 백테스팅
4. 검증

의 무한루프

1. 데이터를 의심하라
2. 미래를 먼저 바라보지 마라
3. 과최적화를 피하라
4. 검증기회는 한번 뿐이다.
5. 시대는 변한다
6. 퀀트도 자신을 절제해야한다.
7. 비퀀트적 언어로 전략을 설명하기
8. 나를 여러각도에서 의심하라
9. 벤치마크를 제대로 설정하라
10. 전략을 분산하라

"""


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



