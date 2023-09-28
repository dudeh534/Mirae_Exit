from pykrx import stock
import pandas as pd
import numpy as np
import random
import os
import time
import matplotlib.pyplot as plt

class MovingAverageStrategy:

    def double_moving_average_yield(self, df, Ns, Nl):
        df = df[['종가']].copy()
        df['ma_s'] = df['종가'].rolling(Ns).mean().shift(1)
        df['ma_l'] = df['종가'].rolling(Nl).mean().shift(1)

        cond = (df['ma_s'] > df['ma_l']) & (df['ma_l'].pct_change() > 0)
        df['status'] = np.where(cond, 1, 0)
        df.at[df.index[-1], 'status'] = 0

        buy_signal = (df['status'] == 1) & (df['status'].shift(1) != 1)
        sell_signal = (df['status'] == 0) & (df['status'].shift(1) == 1)

        earning = df.loc[sell_signal, '종가'].reset_index(drop=True) / df.loc[buy_signal, '종가'].reset_index(drop=True)
        earning = earning - 0.002
        return earning.cumprod().iloc[-1]

    def single_moving_average_yield(self, df, Ns):
        df = df[['종가']].copy()
        df['ma'] = df['종가'].rolling(Ns).mean().shift(1)

        cond = (df['종가'] > df['ma']) & (df['ma'].pct_change() > 0)
        df['status'] = np.where(cond, 1, 0)
        df.iloc[-1, -1] = 0

        buy_signal = (df['status'] == 1) & (df['status'].shift(1) != 1)
        sell_signal = (df['status'] == 0) & (df['status'].shift(1) == 1)

        signal_df = pd.DataFrame()

        signal_df['buy'] = buy_signal
        signal_df['sell'] = sell_signal

        earning = df.loc[sell_signal, '종가'].reset_index(drop=True) / df.loc[buy_signal, '종가'].reset_index(drop=True)
        earning = earning - 0.002
        return earning.cumprod().iloc[-1]

    def double_moving_average_yield_with_bollinger(self, df, Ns, Nl):
        df = df[['종가', '거래량']].copy()

        # 이동평균선 계산
        df['ma_s'] = df['종가'].rolling(Ns).mean().shift(1)
        df['ma_l'] = df['종가'].rolling(Nl).mean().shift(1)

        # 볼린저 밴드 계산
        df['std'] = df['종가'].rolling(Ns).std().shift(1)
        df['bollinger_upper'] = df['ma_s'] + (2 * df['std'])
        df['bollinger_lower'] = df['ma_s'] - (2 * df['std'])

        # 거래량 이동평균선 계산
        df['volume_ma'] = df['거래량'].rolling(Ns).mean().shift(1)

        cond1 = (df['ma_s'] > df['ma_l']) & (df['ma_l'].pct_change() > 0)
        cond2 = (df['거래량'] > df['volume_ma'] * 2)

        df['status'] = np.where(cond1 & cond2, 1, 0)
        df.at[df.index[-1], 'status'] = 0

        buy_signal = (df['status'] == 1) & (df['status'].shift(1) != 1)
        sell_signal = (df['status'] == 0) & (df['status'].shift(1) == 1)

        earning = df.loc[sell_signal, '종가'].reset_index(drop=True) / df.loc[buy_signal, '종가'].reset_index(drop=True)
        earning = earning - 0.002

        return earning.cumprod().iloc[-1]
