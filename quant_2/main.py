import backtrader as bt
from pykrx import stock
import pandas as pd


class SmaCross(bt.Strategy):  # bt.Strategy를 상속한 class로 생성해야 함.
    # 볼린저 밴드에 사용할 이동평균 일 수와 표준편차에 곱할 상수를 정의합니다.
    params = (
        ("period", 20),
        ("devfactor", 3),
        ("debug", False)
    )

    # 프롬프트에 매수 or 매도, 매수매도 가격, 개수를 출력합니다.
    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.data.datetime[0]
        if isinstance(dt, float):
            dt = bt.num2date(dt)
        print('%s, %s' % (dt.isoformat(), txt))

    # 볼린저 밴드 indicators를 가져옵니다.
    def __init__(self):
        self.boll = bt.indicators.BollingerBands(period=self.p.period, devfactor=self.p.devfactor, plot=True)

    def next(self):
        global size
        # 매수한 종목이 없다면,
        if not self.position:  # not in the market
            # 저가 < 볼린저 밴드 하한선 이면,
            if self.data.low[0] < self.boll.lines.bot[0]:
                bottom = self.boll.lines.bot[0]
                # size는 매수 또는 매도 개수로, 현재 금액에서 타겟가격으로 나누어줍니다.
                # 볼린저 밴드 하한선에서 구매 시 최대 구매 가능 개수
                size = int(self.broker.getcash() / bottom)  # 최대 구매 가능 개수
                self.buy(price=bottom, size=size)  # 매수 size = 구매 개수 설정
                self.log('BUY CREATE, %.2f' % (bottom))
                print(size, 'EA')
        else:
            # 고가 > 볼린저 밴드 중간선이면,
            if self.data.high[0] > self.boll.lines.mid[0]:
                self.sell(price=self.boll.lines.mid[0], size=size)  # 매도
                self.log('SELL CREATE, %.2f' % (self.boll.lines.mid[0]))
                print(size, 'EA')

size=0
stock_name = "KODEX 200"
stock_from = "20201125"
stock_to = "20230525"

# 전체 종목코드와 종목명 가져오기
stock_list = pd.DataFrame({'종목코드':stock.get_etf_ticker_list(stock_to)})
stock_list['종목명'] = stock_list['종목코드'].map(lambda x: stock.get_etf_ticker_name(x))
stock_list.head()

ticker = stock_list.loc[stock_list['종목명']==stock_name, '종목코드']
df = stock.get_etf_ohlcv_by_date(fromdate=stock_from, todate=stock_to, ticker=ticker)
df = df.drop(['NAV','거래대금','기초지수'], axis=1)
df = df.rename(columns={'시가':'open', '고가':'high', '저가':'low', '종가':'close', '거래량':'volume'})

df["open"]=df["open"].apply(pd.to_numeric,errors="coerce")
df["high"]=df["high"].apply(pd.to_numeric,errors="coerce")
df["low"]=df["low"].apply(pd.to_numeric,errors="coerce")
df["close"]=df["close"].apply(pd.to_numeric,errors="coerce")
df["volume"]=df["volume"].apply(pd.to_numeric,errors="coerce")

data = bt.feeds.PandasData(dataname=df)
cerebro = bt.Cerebro()  # create a "Cerebro" engine instance
cerebro.broker.setcash(1000000)
cerebro.broker.setcommission(0.00015)   #0.015% 수수료

cerebro.adddata(data)  # Add the data feed
cerebro.addstrategy(SmaCross)  # Add the trading strategy
cerebro.run()  # run it all
cerebro.plot(style='candlestick',barup='red',bardown='blue',xtight=True,ytight=True, grid=True)  # and plot it with a single command