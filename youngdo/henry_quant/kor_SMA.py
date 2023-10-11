import pandas_datareader as web
import talib
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import statsmodels.api as sm
from scipy.stats import zscore
import pymysql
from tqdm import tqdm

engine = create_engine('mysql+pymysql://root:Myhome469!@127.0.0.1:3306/stock_db')
con = pymysql.connect(user='root',
                      passwd='Myhome469!',
                      host='127.0.0.1',
                      db='stock_db',
                      charset='utf8')

mycursor = con.cursor()

ticker_list = pd.read_sql("""
select * from kor_ticker
where 기준일 = (select max(기준일) from kor_ticker) 
	and 종목구분 = '보통주';
""", con=engine)

price_list = pd.read_sql("""
select 날짜, 종가, 종목코드
from kor_price;
""", con=engine)


query = """
    update kor_price set SMA_5 = %s, SMA_10 = %s, SMA_20 = %s, SMA_60 = %s
    where 날짜 = %s and 종목코드 = %s;
"""

# 오류 발생시 저장할 리스트 생성
error_list = []
# 전종목 주가 다운로드 및 저장
for i in tqdm(range(0, len(ticker_list))):
    temp_data = pd.DataFrame()
    price = pd.DataFrame()
    temp_data1 = pd.DataFrame()
    ticker = ticker_list['종목코드'][i]
    temp_data = price_list[price_list['종목코드'] == ticker]
    temp_data1 = temp_data.copy()
    try:

        temp_data1['SMA_5'] = talib.SMA(temp_data['종가'],
                                                timeperiod=5).round(0)  # 20일 단순 이동평균

        temp_data1['SMA_10'] = talib.SMA(temp_data['종가'],
                                         timeperiod=10).round(0)  # 20일 단순 이동평균

        temp_data1['SMA_20'] = talib.SMA(temp_data['종가'],
                                         timeperiod=20).round(0)  # 20일 단순 이동평균

        temp_data1['SMA_60'] = talib.SMA(temp_data['종가'],
                                         timeperiod=60).round(0)  # 20일 단순 이동평균

        temp_data1 = temp_data1.fillna(0)

        price = temp_data1[['SMA_5', 'SMA_10', 'SMA_20', 'SMA_60', '날짜', '종목코드' ]]


        # 주가 데이터를 DB에 저장
        args = price.values.tolist()
        mycursor.executemany(query, args)
        con.commit()

    except:

        # 오류 발생시 error_list에 티커 저장하고 넘어가기
        print(ticker)
        error_list.append(ticker)

# DB 연결 종료
engine.dispose()
con.close()


"""
todo
talib
pykrx 이용해서 강남기법 구현하기

굿
1. 이평선 밀집, 종가가 5, 10 20 일선 위 위치
2. 사각형 공간 20일 ~ 40일 사이 위치
3. 7월 12일 ~ 7월 31일 사이 외국인, 기관 연기금 수급
4. 최근 4분기 영업이익 증가추이
5. 코스피 300 이내 종목
6. 우크라이나 재건 테마 재료 유지

배드
1. KOSPI 역배열 분위기 
2. 최적의 타이밍은 9/15 ~ 9/16
3. 거래량?
"""