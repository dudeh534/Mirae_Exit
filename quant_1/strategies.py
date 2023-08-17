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
