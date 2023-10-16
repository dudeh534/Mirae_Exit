import pymysql
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import statsmodels.api as sm
from scipy.stats import zscore
import matplotlib.pyplot as plt
from datetime import date
import warnings
warnings.filterwarnings('ignore')

engine = create_engine('mysql+pymysql://root:Myhome469!@127.0.0.1:3306/stock_db')

ticker_list = pd.read_sql("""
select * from kor_ticker
where 기준일 = (select max(기준일) from kor_ticker) 
	and 종목구분 = '보통주';
""", con=engine)

op_list = pd.read_sql("""
select * from kor_fs
where 계정 = '영업이익'
and 공시구분 = 'q'
order by 계정, 종목코드, 기준일;
""", con=engine)

np_list = pd.read_sql("""
select * from kor_fs
where 계정 = '당기순이익'
and 공시구분 = 'q'
order by 계정, 종목코드, 기준일;
""", con=engine)

value_list = pd.read_sql("""
select * from kor_value
where 기준일 = (select max(기준일) from kor_value);
""", con=engine)

sector_list = pd.read_sql("""
select CMP_CD as 종목코드, CMP_KOR, SEC_NM_KOR from kor_sector
where 기준일 = (select max(기준일) from kor_sector);
""", con=engine)

price_list = pd.read_sql("""
SELECT price.종목코드, price.종가, price.SMA_5, price.SMA_10, price.SMA_20
FROM kor_ticker as ticker
JOIN kor_price as price
on ticker.종목코드 = price.종목코드
where ticker.기준일 = (select max(기준일) from kor_ticker)
and price.날짜 = (select max(날짜) from kor_price) 
and ticker.market in ('1002', '1003', '2203');
""", con=engine)

volume_list = pd.read_sql("""
select 종목코드, sum((금융투자+보험+투신+사모+은행+기타금융+연기금+기타법인)) as 기관계, sum((투신+연기금)) as 연기금, sum((외국인+기타외국인)) as 외국인, sum(개인) as 개인
from kor_volume
where 날짜 >= (select (select max(날짜) from kor_volume) - interval 3 month)
group by 종목코드;
""", con=engine)

engine.dispose()

pd.set_option('display.max_columns', None)  ## 모든 열을 출력한다.

# 종가('종가')가 모든 이평선('SMA_5', 'SMA_10', 'SMA_20')보다 낮은 종목코드 추출
under_sma = price_list[(price_list['종가'] > price_list['SMA_5']) & (price_list['종가'] > price_list['SMA_10']) & (price_list['종가'] > price_list['SMA_20'])]

# 종가와 이평선들 간의 차이의 절대값 계산
under_sma['SMA_5_diff'] = np.abs(under_sma['종가'] - under_sma['SMA_5'])
under_sma['SMA_10_diff'] = np.abs(under_sma['종가'] - under_sma['SMA_10'])
under_sma['SMA_20_diff'] = np.abs(under_sma['종가'] - under_sma['SMA_20'])

# 5% 미만 범위에 위치하는 종목코드 추출
under_5_percent = under_sma[(under_sma['SMA_5_diff'] / under_sma['종가'] < 0.05) &
                    (under_sma['SMA_10_diff'] / under_sma['종가'] < 0.05) &
                    (under_sma['SMA_20_diff'] / under_sma['종가'] < 0.05)]['종목코드']

# 결과 출력
print("SMA_5, SMA_10, SMA_20 모두 이평선 아래에 위치한 종목:", len(under_5_percent))

# under_5_percent에 저장된 종목코드를 기준으로 데이터프레임 필터링
filtered_data = volume_list[volume_list['종목코드'].isin(under_5_percent)]

filtered_data1 = filtered_data[(filtered_data['연기금'] > 1 * filtered_data['개인']) | (filtered_data['기관계'] > 1 * filtered_data['개인']) | (filtered_data['외국인'] > 1 * filtered_data['개인'])]

print("수급포함:", len(filtered_data1))

# 두 데이터프레임을 종목코드를 기준으로 합침
merged_df = filtered_data1.merge(sector_list, on='종목코드', how='inner')

# 결과 출력
print(merged_df)

# 날짜를 기준으로 정렬
np_list['기준일'] = pd.to_datetime(np_list['기준일'])
np_list = np_list.sort_values(by=['종목코드', '기준일'])

# 종목별로 4분기 연속으로 상승하는지 확인
result = []
for code, group in np_list.groupby('종목코드'):
    if len(group) == 4:
        # 4개의 분기 데이터가 있는 경우
        profits = group['값'].tail(3).values
        if all(profits[i] < profits[i + 1] for i in range(2)):
            result.append(code)

# 날짜를 기준으로 정렬
op_list['기준일'] = pd.to_datetime(op_list['기준일'])
op_list = op_list.sort_values(by=['종목코드', '기준일'])


for code, group in op_list.groupby('종목코드'):
    if len(group) == 4:
        # 4개의 분기 데이터가 있는 경우
        profits = group['값'].tail(3).values
        if all(profits[i] < profits[i + 1] for i in range(2)):
            result.append(code)

filtered_data2 = filtered_data1[filtered_data1['종목코드'].isin(result)]

print("수급포함:", len(filtered_data1))

# 두 데이터프레임을 종목코드를 기준으로 합침
merged_df1 = filtered_data2.merge(sector_list, on='종목코드', how='inner')

# 결과 출력
print(merged_df1)

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

#데이짱 책 정독 - 기법 체화
#우량성, 이익성 지표 발굴 및 추가 
#백테스팅? 계획 재수립
#순이익률도 계산
"""