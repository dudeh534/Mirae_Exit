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

query = """
    update kor_ticker set market = %s
    where 종목코드 = %s;
"""

"""
1002 코스피 대형주
1003 코스피 중형주
2203 코스닥 150
"""
for index in ['1002', '1003', '2203']:
    print(index)
    pdf = stock.get_index_portfolio_deposit_file(index)

    for i in pdf:
        temp_data = pd.DataFrame()
        # 주가 데이터를 DB에 저장
        temp_data = pd.DataFrame({
            'market': [index],
            '종목코드': [i]
        })
        args = temp_data.values.tolist()
        mycursor.executemany(query, args)
        con.commit()

# DB 연결 종료
engine.dispose()
con.close()


