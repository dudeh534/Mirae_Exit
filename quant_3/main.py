import backtrader as bt
from pykrx import stock
import pandas as pd
import time


# 데이터 피드 정의
class KRXDataLoader(bt.feeds.PandasData):
    lines = ('volume',)
    params = (('open', '시가'),
              ('high', '고가'),
              ('low', '저가'),
              ('close', '종가'),
              ('volume', '거래량'),
              ('openinterest', None))


# 거래 전략 정의
class VolatilityStrategy(bt.Strategy):
    params = dict(
        volatility_threshold=2.0,
        percent=1.0
    )

    def __init__(self):
        self.order = None

    def next(self):
        mean_volume = self.data.volume[-1]

        # 거래량이 평균보다 threshold 배 크다면
        if self.data.volume[0] > mean_volume * self.params.volatility_threshold:
            target_price = self.data.open[0] * (1 + self.params.percent / 100.0)

            if self.data.close[0] > target_price:
                self.order = self.buy()
            elif self.data.close[0] < target_price:
                self.order = self.sell()


if __name__ == '__main__':

    """

    tickers = stock.get_index_portfolio_deposit_file("1028")

   

    result = []
    for ticker_1034 in tickers:
        cerebro = bt.Cerebro()
        # 데이터 가져오기
        df = stock.get_market_ohlcv_by_date("20200101", "20211231", ticker_1034)
        df.index = pd.to_datetime(df.index)

        data = KRXDataLoader(dataname=df)
        cerebro.adddata(data)

        # 전략 추가
        cerebro.addstrategy(VolatilityStrategy)

        # 백테스팅 시작 자본금 설정
        cerebro.broker.set_cash(1000000)

        print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

        cerebro.run()

        print('Ending Portfolio Value: %.2f' % cerebro.broker.getvalue())
        temp = cerebro.broker.getvalue()
        result.append(temp)
        time.sleep(1)

    s = pd.Series(result, tickers)
    s = s.sort_values(ascending=False)
    print(s)
"""

    cerebro = bt.Cerebro()
    # 데이터 가져오기
    df = stock.get_market_ohlcv_by_date("20200101", "20211231", '005930')
    df.index = pd.to_datetime(df.index)

    data = KRXDataLoader(dataname=df)
    cerebro.adddata(data)

    # 전략 추가
    cerebro.addstrategy(VolatilityStrategy)

    # 백테스팅 시작 자본금 설정
    cerebro.broker.set_cash(1000000)

    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    cerebro.run()
    cerebro.plot(style='candlestick', barup='red', bardown='blue', xtight=True, ytight=True,
                 grid=True)  # and plot it with a single command
    print('Ending Portfolio Value: %.2f' % cerebro.broker.getvalue())



