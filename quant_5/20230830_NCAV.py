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

#재무재표 cleansing
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

#재무재표 가져오기 분기 : q, 연도 : y
def data_fs(ticker, date_tp) :
    url = f'http://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{ticker}'
    data = pd.read_html(url, displayed_only=False)

    if date_tp == 'q' :
        data_fs_q = pd.concat(
            [data[1].iloc[:, ~data[1].columns.str.contains('전년동기')], data[3], data[5]]
        )
        data_fs_q = data_fs_q.rename(columns={data_fs_q.columns[0]: "계정"})
        data_fs_q_clean = clean_fs(data_fs_q, ticker, 'q')

        return data_fs_q_clean

    elif date_tp == 'y' :
        data_fs_y = pd.concat([data[0].iloc[:, ~data[0].columns.str.contains('전년동기')],
                               data[2],
                               data[4]])

        data_fs_y = data_fs_y.rename(columns={data_fs_y.columns[0]: "계정"})

        page_data = rq.get(url)

        page_data_html = BeautifulSoup(page_data.content,  features="lxml")

        fiscal_data = page_data_html.select('div.corp_group1 > h2')
        fiscal_data_text = fiscal_data[1].text
        fiscal_data_text = re.findall('[0-9]+', fiscal_data_text)

        data_fs_y = data_fs_y.loc[:, ((data_fs_y.columns == "계정") | data_fs_y.columns.str[-2:].isin(fiscal_data_text))]

        data_fs_y_clean = clean_fs(data_fs_y, ticker, 'y')

        return data_fs_y_clean

#NCAV Score 가져오기
def NCAV_Score_cal(ticker) :
    try:

        url = f'http://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{ticker}'
        data = pd.read_html(url, displayed_only=False)

        data_fs_q = pd.concat(
            [data[1].iloc[:, ~data[1].columns.str.contains('전년동기')], data[3], data[5]]
        )
        data_fs_q = data_fs_q.rename(columns={data_fs_q.columns[0]: "계정"})
        data_fs_q_clean = clean_fs(data_fs_q, ticker, 'q')
        sample = stock.get_market_cap("20230831", "20230831", ticker, "m")

        ncav_score = ((data_fs_q_clean.loc[data_fs_q_clean['계정'] == '유동자산']['값'].iloc[-1] -
                       data_fs_q_clean.loc[data_fs_q_clean['계정'] == '부채']['값'].iloc[-1]) /
                      sample['시가총액'].iloc[-1] * 100000000)

        return f"{ncav_score:.3f}"

    except:

        return "error"

def F_Score_cal(ticker):


    ds_temp_y = data_fs(ticker, 'y')
    ds_temp_q = data_fs(ticker, 'q')
    #자산수익률(당기순이익)
    roa = ds_temp_y.loc[ds_temp_y['계정'] == '당기순이익']['값'].iloc[-1]

    #영업현금흐름
    cfo = ds_temp_y.loc[ds_temp_y['계정'] == '영업활동으로인한현금흐름']['값'].iloc[-1]

    #전년도 ROA
    delta_roa_bf = ds_temp_y.loc[ds_temp_y['계정'] == '당기순이익']['값'].iloc[-1] / ds_temp_y.loc[ds_temp_y['계정'] == '자산']['값'].iloc[-1] * 100
    #전전년도 ROA
    delta_roa_bfbf = ds_temp_y.loc[ds_temp_y['계정'] == '당기순이익']['값'].iloc[-2] / ds_temp_y.loc[ds_temp_y['계정'] == '자산']['값'].iloc[-2] * 100
    #근데 wisereport 랑 왜 다르게 나오지?
    #ROA의 증가율
    delta_roa = delta_roa_bf - delta_roa_bfbf

    #순이익과 영업현금흐름의 차이
    accrual = roa - cfo

    #당해부채비율
    delta_lever_bf = ds_temp_q.loc[ds_temp_q['계정'] == '부채']['값'].iloc[-1] / ds_temp_q.loc[ds_temp_q['계정'] == '자본']['값'].iloc[-1]
    #전년부채비율
    delta_lever_bfbf = ds_temp_y.loc[ds_temp_y['계정'] == '부채']['값'].iloc[-1] / ds_temp_y.loc[ds_temp_y['계정'] == '자본']['값'].iloc[-1]
    #부채비율증가율
    delta_lever = delta_lever_bf - delta_lever_bfbf

    # 유동성의 변화 : 당해 유동비율
    delta_liquidity_bf = ds_temp_q.loc[ds_temp_q['계정'] == '유동자산']['값'].iloc[-1] / ds_temp_q.loc[ds_temp_q['계정'] == '유동부채']['값'].iloc[-1]
    # 유동성의 변화 : 전년 유동비율
    delta_liquidity_bfbf = ds_temp_y.loc[ds_temp_y['계정'] == '유동자산']['값'].iloc[-1] / ds_temp_y.loc[ds_temp_y['계정'] == '유동부채']['값'].iloc[-1]
    # 유동성의 변화율
    delta_liquidity = delta_liquidity_bf - delta_liquidity_bfbf

    # 작년상장주식수
    stock_2022 = stock.get_market_cap("20221201", "20221231", ticker, "m")
    stock_2022_last = stock_2022['상장주식수'].iloc[-1]
    # 올해상장주식수
    stock_2023 = stock.get_market_cap("20230801", "20230831", ticker, "m")
    stock_2023_last = stock_2023['상장주식수'].iloc[-1]
    # 두 상장주식수 비교
    eq_offer = 1 if stock_2023_last > stock_2022_last else 0

    # 매출총이익률의 변화 : 당해 매출총이익율
    delta_margin_bf = ds_temp_q.loc[ds_temp_q['계정'] == '매출총이익']['값'].iloc[-1] / ds_temp_q.loc[ds_temp_q['계정'] == '매출액']['값'].iloc[-1]
    # 매출총이익률의 변화 : 전년 매출총이익율
    delta_margin_bfbf = ds_temp_y.loc[ds_temp_y['계정'] == '매출총이익']['값'].iloc[-1] / ds_temp_y.loc[ds_temp_y['계정'] == '매출액']['값'].iloc[-1]
    # 매출총이익률의 변화
    delta_margin = delta_margin_bf - delta_margin_bfbf

    # 자산회전율의 변화 : 당해 자산회전율
    delta_turn_bf = ds_temp_q.loc[ds_temp_q['계정'] == '매출액']['값'].iloc[-1] / ds_temp_q.loc[ds_temp_q['계정'] == '자산']['값'].iloc[-1]
    # 자산회전율의 변화 : 전년 자산회전율
    delta_turn_bfbf = ds_temp_y.loc[ds_temp_y['계정'] == '매출액']['값'].iloc[-1] / ds_temp_y.loc[ds_temp_y['계정'] == '자산']['값'].iloc[-1]
    # 자산회전율의 변화
    delta_turn = delta_turn_bf - delta_turn_bfbf

    # F-Score 계산
    F_Score = 0
    F_Score += 1 if roa > 0 else 0
    F_Score += 1 if cfo > 0 else 0
    F_Score += 1 if delta_roa > 0 else 0
    F_Score += 1 if accrual < cfo else 0
    F_Score += 1 if delta_lever < 0 else 0
    F_Score += 1 if delta_liquidity > 0 else 0
    F_Score += 1 if eq_offer == 0 else 0
    F_Score += 1 if delta_margin > 0 else 0
    F_Score += 1 if delta_turn > 0 else 0

    return F_Score

def GPA_cal(ticker):
    ds_temp_q = data_fs(ticker, 'q')
    return ds_temp_q.loc[ds_temp_q['계정'] == '매출총이익']['값'].iloc[-1] / ds_temp_q.loc[ds_temp_q['계정'] == '자산']['값'].iloc[-1]

if __name__ == "__main__":
    combined_df = combine_excel_files("kospi_tickers.xlsx", "kosdaq_tickers.xlsx")

    num = int(input("Enter a number: "))

    if num == 1:

        df_to_save = pd.DataFrame(columns=['Ticker', 'NCAV_Score', 'Name', 'F_Score', 'GP/A'])

        temp = 0
        for ticker in combined_df['종목코드']:
            ncav_score= NCAV_Score_cal(ticker)

            if ncav_score >= 0.5 :
                # 새로운 행을 DataFrame으로 만들고 concat으로 추가
                new_row = pd.DataFrame({
                    'Ticker': [ticker],
                    'NCAV_Score': [ncav_score],
                    'Name': [combined_df.loc[combined_df['종목코드'] == ticker]['종목명'].item()],
                    'F_Score':[F_Score_cal(ticker)],
                    'GP/A':[GPA_cal(ticker)]
                })

            df_to_save = pd.concat([df_to_save, new_row], ignore_index=True)

            time.sleep(2)

        print(df_to_save)
        df_to_save.to_excel("NCAV_Score.xlsx", index=False)

    elif num == 2:
        print(F_Score_cal('035720'))

    else :
        pass


