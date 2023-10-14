import pymysql
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import statsmodels.api as sm
from scipy.stats import zscore
import matplotlib.pyplot as plt
from datetime import date

"""
퀄리티: 자기자본이익률(ROE), 매출총이익(GPA), 영업활동현금흐름(CFO)
밸류: PER, PBR, PSR, PCR, DY
모멘텀: 12개월 수익률, K-Ratio
"""

#cutoff 즉 아웃라이어는 1%로 설정하며, asc는 False로 설정한다.
def col_clean(df, cutoff=0.01, asc=False):

    #아웃라이어 기준에 해당하는 q_low와 q_hi을 계산한다.
    q_low = df.quantile(cutoff)
    q_hi = df.quantile(1 - cutoff)

    #트림 방법을 통해 이상치 데이터를 제외한 값을 선택한다.
    df_trim = df[(df > q_low) & (df < q_hi)]

    #만일 asc가 False일 경우 순위를 ascending = False 즉 내림차순으로 계산한다. 만일 asc가 True일 경우에는 순위를 ascending = True 즉 오름차순으로 계산한다.
    #그 후 apply() 메서드를 통해 zscore를 계산한다.
    if asc == False:
        df_z_score = df_trim.rank(axis=0, ascending=False).apply(
            zscore, nan_policy='omit')
    if asc == True:
        df_z_score = df_trim.rank(axis=0, ascending=True).apply(
            zscore, nan_policy='omit')

    return(df_z_score)


engine = create_engine('mysql+pymysql://root:Myhome469!@127.0.0.1:3306/stock_db')

ticker_list = pd.read_sql("""
select * from kor_ticker
where 기준일 = (select max(기준일) from kor_ticker) 
	and 종목구분 = '보통주';
""", con=engine)

fs_list = pd.read_sql("""
select * from kor_fs
where 계정 in ('당기순이익', '매출총이익', '영업활동으로인한현금흐름', '자산', '자본')
and 공시구분 = 'q';
""", con=engine)

value_list = pd.read_sql("""
select * from kor_value
where 기준일 = (select max(기준일) from kor_value);
""", con=engine)

price_list = pd.read_sql("""
select 날짜, 종가, 종목코드
from kor_price
where 날짜 >= (select (select max(날짜) from kor_price) - interval 1 year);
""", con=engine)

sector_list = pd.read_sql("""
select * from kor_sector
where 기준일 = (select max(기준일) from kor_ticker);	
""", con=engine)

engine.dispose()

fs_list = fs_list.sort_values(['종목코드', '계정', '기준일'])
fs_list['ttm'] = fs_list.groupby(['종목코드', '계정'], as_index=False)['값'].rolling(
    window=4, min_periods=4).sum()['값']
fs_list_clean = fs_list.copy()
fs_list_clean['ttm'] = np.where(fs_list_clean['계정'].isin(['자산', '지배기업주주지분']),
                                fs_list_clean['ttm'] / 4, fs_list_clean['ttm'])
fs_list_clean = fs_list_clean.groupby(['종목코드', '계정']).tail(1)

fs_list_pivot = fs_list_clean.pivot(index='종목코드', columns='계정', values='ttm')
fs_list_pivot['ROE'] = fs_list_pivot['당기순이익'] / fs_list_pivot['자본']
fs_list_pivot['GPA'] = fs_list_pivot['매출총이익'] / fs_list_pivot['자산']
fs_list_pivot['CFO'] = fs_list_pivot['영업활동으로인한현금흐름'] / fs_list_pivot['자산']

value_list.loc[value_list['값'] <= 0, '값'] = np.nan
value_pivot = value_list.pivot(index='종목코드', columns='지표', values='값')


#먼저 가격 테이블을 이용해 최근 12개월 수익률을 구한다.
price_pivot = price_list.pivot(index='날짜', columns='종목코드', values='종가')
ret_list = pd.DataFrame(data=(price_pivot.iloc[-1] / price_pivot.iloc[0]) - 1,
                        columns=['12M'])

#로그 누적수익률을 통해 각 종목 별 K-Ratio를 계산한다.
ret = price_pivot.pct_change().iloc[1:]
ret_cum = np.log(1 + ret).cumsum()

x = np.array(range(len(ret)))
k_ratio = {}

for i in range(0, len(ticker_list)):

    ticker = ticker_list.loc[i, '종목코드']

    try:
        y = ret_cum.loc[:, price_pivot.columns == ticker]
        reg = sm.OLS(y, x).fit()
        res = float(reg.params / reg.bse)
    except:
        res = np.nan

    k_ratio[ticker] = res

k_ratio_bind = pd.DataFrame.from_dict(k_ratio, orient='index').reset_index()
k_ratio_bind.columns = ['종목코드', 'K_ratio']

#티커, 섹터, 퀄리티, 밸류, 12개월 수익률, K-ratio 테이블을 하나로 합친다.
data_bind = ticker_list[['종목코드', '종목명']].merge(
    sector_list[['CMP_CD', 'SEC_NM_KOR']],
    how='left',
    left_on='종목코드',
    right_on='CMP_CD').merge(
        fs_list_pivot[['ROE', 'GPA', 'CFO']], how='left',
        on='종목코드').merge(value_pivot, how='left',
                         on='종목코드').merge(ret_list, how='left',
                                          on='종목코드').merge(k_ratio_bind,
                                                           how='left',
                                                           on='종목코드')

data_bind.loc[data_bind['SEC_NM_KOR'].isnull(), 'SEC_NM_KOR'] = '기타'
data_bind = data_bind.drop(['CMP_CD'], axis=1)

#먼저 종목코드와 섹터정보(SEC_NM_KOR)를 인덱스로 설정한 후, 섹터에 따른 그룹을 묶어준다.
data_bind_group = data_bind.set_index(['종목코드',
                                       'SEC_NM_KOR']).groupby('SEC_NM_KOR')

#첫번째로 퀄리티 지표의 Z-Score를 계산해보도록 하자.
#섹터별 그룹으로 묶인 테이블에서 퀄리티 지표에 해당하는 ROE, GPA, CFO 열을 선택한 후, 위에서 만든 col_clean() 함수를 적용하면 아웃라이어를 제거한 후 순위의 Z-Score를 계산한다
#sum 함수를 통해 Z-Score의 합을 구하며, to_frame() 메서드를 통해 데이터프레임 형태로 변경한다.
z_quality = data_bind_group[['ROE', 'GPA', 'CFO'
                             ]].apply(lambda x: col_clean(x, 0.01, False)).sum(
                                 axis=1, skipna=False).to_frame('z_quality')

#data_bind 테이블과 합치며, z_quality 열에는 퀄리티 지표의 Z-Score가 표시된다.
data_bind = data_bind.merge(z_quality, how='left', on=['종목코드', 'SEC_NM_KOR'])


#밸류 지표에 해당하는 PBR, PCR, PER, PSR 열을 선택한 후, col_clean() 함수를 적용한다. 또한 인자에 True를 입력해 오름차순으로 순위를 구한다.
value_1 = data_bind_group[['PBR', 'PCR', 'PER',
                           'PSR']].apply(lambda x: col_clean(x, 0.01, True))
#DY(배당수익률)의 경우 내림차순으로 순위를 계산해야 하므로 col_clean() 함수에 False를 입력한다.
value_2 = data_bind_group[['DY']].apply(lambda x: col_clean(x, 0.01, False))
#위의 두 결과에서 나온 합쳐 Z-Score의 합을 구한 후, 데이터프레임 형태로 변경한다.
z_value = value_1.merge(value_2, on=['종목코드', 'SEC_NM_KOR'
                                     ]).sum(axis=1,
                                            skipna=False).to_frame('z_value')

#data_bind 테이블과 합치며, z_value 열에는 밸류 지표의 Z-Score가 표시된다.
data_bind = data_bind.merge(z_value, how='left', on=['종목코드', 'SEC_NM_KOR'])

#모멘텀 지표에 해당하는 12M, K_ratio 열을 선택한 후 col_clean() 함수를 적용한다.
z_momentum = data_bind_group[[
    '12M', 'K_ratio'
]].apply(lambda x: col_clean(x, 0.01, False)).sum(
    axis=1, skipna=False).to_frame('z_momentum')

#data_bind 테이블과 합치며, z_momentum 열에는 모멘텀 지표의 Z-Score가 표시된다.
data_bind = data_bind.merge(z_momentum, how='left', on=['종목코드', 'SEC_NM_KOR'])

data_z = data_bind[['z_quality', 'z_value', 'z_momentum']].copy()

data_bind_final = data_bind[['종목코드', 'z_quality', 'z_value', 'z_momentum'
                             ]].set_index('종목코드').apply(zscore,
                                                        nan_policy='omit')
data_bind_final.columns = ['quality', 'value', 'momentum']


#각 팩터별 비중을 리스트로 만들며, 0.3으로 동일한 비중을 입력한다. 비중을 [0.2, 0.4, 0.4]와 같이 팩터별로 다르게 줄 수도 있으며,
#이는 어떠한 팩터를 더욱 중요하게 생각하는지 혹은 더욱 좋게 보는지에 따라 조정이 가능하다.
wts = [0.3, 0.3, 0.3]

#팩터별 Z-Score와 비중의 곱을 구한 후 이를 합하며, 데이터프레임(data_bind_final_sum) 형태로 변경한다.
data_bind_final_sum = (data_bind_final * wts).sum(axis=1,
                                                  skipna=False).to_frame()
data_bind_final_sum.columns = ['qvm']
#기존 테이블(data_bind)과 합친다.
port_qvm = data_bind.merge(data_bind_final_sum, on='종목코드')
#최종 Z-Score의 합(qvm) 기준 순위가 1~20인 경우는 투자 종목에 해당하므로 ‘Y’, 그렇지 않으면 ‘N’으로 표시한다.
port_qvm['invest'] = np.where(port_qvm['qvm'].rank() <= 20, 'Y', 'N')

print(port_qvm[port_qvm['invest'] == 'Y'].round(4))

port_qvm[port_qvm['invest'] == 'Y'].round(4).to_excel('model.xlsx', index=False)

# DB 연결
engine = create_engine('mysql+pymysql://root:Myhome469!@127.0.0.1:3306/stock_db')
con = pymysql.connect(user='root',
                      passwd='Myhome469!',
                      host='127.0.0.1',
                      db='stock_db',
                      charset='utf8')
mycursor = con.cursor()

# DB 저장 쿼리
query = """
    insert into model_result (날짜, 종목코드, 종목명, SEC_NM_KOR, ROE, GPA, CFO, DY, PBR, PCR, PER, PSR, 12M, K_ratio, z_quality, z_value, z_momentum, qvm, invest)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
    on duplicate key update
    종목명 = new.종목명, SEC_NM_KOR = new.SEC_NM_KOR, ROE = new.ROE, GPA = new.GPA, CFO = new.CFO, DY = new.DY,
    PBR = new.PBR, PCR = new.PCR, PER = new.PER, PSR = new.PSR, 12M = new.12M, K_ratio = new.K_ratio, z_quality = new.z_quality,
    z_value = new.z_value, z_momentum = new.z_momentum, qvm = new.qvm, invest = new.invest;
"""

port_qvm.insert(0, '날짜', pd.to_datetime((date.today()).strftime("%Y%m%d")))
args = port_qvm[port_qvm['invest'] == 'Y'].round(4).values.tolist()
mycursor.executemany(query, args)
con.commit()

# DB 연결 종료
engine.dispose()
con.close()