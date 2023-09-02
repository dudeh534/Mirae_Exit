"""
전략 1.  NCAV

- ( 유동자산 - 총부채 ) > 시가총액 * 1.5
- 분기순이익 > 0
- (유동자산 - 총부채) / 시가총액 비중이 가장 높은 주식 매수
- GP/A, F Score 로 순위를 매겨 사용
- 4월, 10월 마지막 거래일에 리밸런싱
https://youtu.be/GzFxVYQvVcA?si=YozTf1SpaAm5MvzC
- 연복리 20%
"""

from pykrx import stock
import pandas as pd
import numpy as np
import requests as rq
from bs4 import BeautifulSoup
import re
import time


def save_ticker_to_excel(market, filename):
    tickers = stock.get_market_ticker_list(market=market) 
    info_list = []

    for ticker in tickers:
        info = stock.get_market_ticker_name(ticker)
        info_list.append({"종목코드": ticker, "종목명": info})

    df = pd.DataFrame(info_list)
    df.to_excel(filename, index=False)

def combine_excel_files(file1, file2):
    # 각각의 엑셀 파일을 DataFrame으로 읽어옴
    df1 = pd.read_excel(file1)
    df1['시장'] = 'KOSPI'

    df2 = pd.read_excel(file2)
    df2['시장'] = 'KOSDAQ'

    # 두 DataFrame을 하나로 합침
    combined_df = pd.concat([df1, df2], ignore_index=True)

    return combined_df

def clean_fs(df, ticker, frequency) :

  df = df[~df.loc[:, ~df.columns.isin(['계정'])].isna().all(axis=1)]
  df = df.drop_duplicates(['계정'], keep='first')
  df = pd.melt(df, id_vars='계정', var_name='기준일', value_name='값')
  df = df[~pd.isnull(df['값'])]
  df['계정'] = df['계정'].replace({'계산에 참여한 계정 펼치기':''}, regex=True)
  df['기준일'] = pd.to_datetime(df['기준일'],
                             format='%Y-%m') + pd.tseries.offsets.MonthEnd()
  df['종목코드'] = ticker
  df['공시구분'] = frequency

  return df

def NCAV_Score_cal(ticker) :
    try:

        url = f'http://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{ticker}'
        data = pd.read_html(url, displayed_only=False)

        data_fs_q = pd.concat(
            [data[1].iloc[:, ~data[1].columns.str.contains('전년동기')], data[3], data[5]]
        )
        data_fs_q = data_fs_q.rename(columns={data_fs_q.columns[0]: "계정"})
        data_fs_q_clean = clean_fs(data_fs_q, 'ticker', 'q')
        sample = stock.get_market_cap("20230831", "20230831", ticker, "m")

        ncav_score = ((data_fs_q_clean.loc[data_fs_q_clean['계정'] == '유동자산']['값'].iloc[-1] -
                       data_fs_q_clean.loc[data_fs_q_clean['계정'] == '부채']['값'].iloc[-1]) /
                      sample['시가총액'].iloc[-1] * 100000000)

        return f"{ncav_score:.3f}"

    except:

        return "error"

if __name__ == "__main__":
    combined_df = combine_excel_files("kospi_tickers.xlsx", "kosdaq_tickers.xlsx")

    df_to_save = pd.DataFrame(columns=['Ticker', 'NCAV_Score', 'Name'])

    temp = 0
    for ticker in combined_df['종목코드']:

        # 새로운 행을 DataFrame으로 만들고 concat으로 추가
        new_row = pd.DataFrame({
            'Ticker': [ticker],
            'NCAV_Score': [NCAV_Score_cal(ticker)],
            'Name': [combined_df.loc[combined_df['종목코드'] == ticker]['종목명'].item()]
        })

        df_to_save = pd.concat([df_to_save, new_row], ignore_index=True)

        time.sleep(2)

    print(df_to_save)
    df_to_save.to_excel("NCAV_Score.xlsx", index=False)


