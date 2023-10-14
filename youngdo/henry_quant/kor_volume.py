from pykrx import stock
import pymysql
from sqlalchemy import create_engine
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
import requests as rq
import time
from tqdm import tqdm
from io import BytesIO

# DB 연결
engine = create_engine('mysql+pymysql://root:Myhome469!@127.0.0.1:3306/stock_db')
con = pymysql.connect(user='root',
                      passwd='Myhome469!',
                      host='127.0.0.1',
                      db='stock_db',
                      charset='utf8')
mycursor = con.cursor()

# 티커리스트 불러오기
ticker_list = pd.read_sql("""
select * from kor_ticker
where 기준일 = (select max(기준일) from kor_ticker) 
	and 종목구분 = '보통주';
""", con=engine)

# DB 저장 쿼리
query = """
    insert into kor_volume (날짜,종목코드,금융투자,보험,투신,사모,은행,기타금융,연기금,기타법인,개인,외국인,기타외국인)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
    on duplicate key update
    금융투자 = new.금융투자, 보험 = new.보험, 투신 = new.투신, 사모 = new.사모, 은행 = new.은행, 기타금융 = new.기타금융,
    연기금 = new.연기금, 기타법인 = new.기타법인, 개인 = new.개인, 외국인 = new.외국인, 기타외국인 = new.기타외국인;
"""
# 오류 발생시 저장할 리스트 생성
error_list = []

# 전종목 주가 다운로드 및 저장
for i in tqdm(range(0, len(ticker_list))):

    # 티커 선택
    ticker = ticker_list['종목코드'][i]

    # 시작일과 종료일

    fr = (date.today() + relativedelta(days=-365)).strftime("%Y%m%d")
    to = (date.today()).strftime("%Y%m%d")

    # 오류 발생 시 이를 무시하고 다음 루프로 진행
    try:

        volume = stock.get_market_trading_volume_by_date(fr, to, ticker, etf=True, etn=True, elw=True,
                                                         detail=True)
        volume = volume.reset_index()
        volume = volume.drop('전체', axis=1)
        volume.insert(1, '종목코드', ticker)
        args = volume.values.tolist()
        mycursor.executemany(query, args)
        con.commit()

    except:

        # 오류 발생시 error_list에 티커 저장하고 넘어가기
        print(ticker)
        error_list.append(ticker)

    # 타임슬립 적용
    time.sleep(0.5)

# DB 연결 종료
engine.dispose()
con.close()



