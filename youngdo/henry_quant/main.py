from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import statsmodels.api as sm
from scipy.stats import zscore
import matplotlib.pyplot as plt


def col_clean(df, cutoff=0.01, asc=False):

    q_low = df.quantile(cutoff)
    q_hi = df.quantile(1 - cutoff)

    df_trim = df[(df > q_low) & (df < q_hi)]

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

print(fs_list_pivot.round(4).head())

value_list.loc[value_list['값'] <= 0, '값'] = np.nan
value_pivot = value_list.pivot(index='종목코드', columns='지표', values='값')

print(value_pivot.head())

price_pivot = price_list.pivot(index='날짜', columns='종목코드', values='종가')
ret_list = pd.DataFrame(data=(price_pivot.iloc[-1] / price_pivot.iloc[0]) - 1,
                        columns=['12M'])

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

print(k_ratio_bind.head())

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

print(data_bind.round(4).head())

data_bind_group = data_bind.set_index(['종목코드',
                                       'SEC_NM_KOR']).groupby('SEC_NM_KOR')

print(data_bind_group.head(1).round(4))

z_quality = data_bind_group[['ROE', 'GPA', 'CFO'
                             ]].apply(lambda x: col_clean(x, 0.01, False)).sum(
                                 axis=1, skipna=False).to_frame('z_quality')
data_bind = data_bind.merge(z_quality, how='left', on=['종목코드', 'SEC_NM_KOR'])


value_1 = data_bind_group[['PBR', 'PCR', 'PER',
                           'PSR']].apply(lambda x: col_clean(x, 0.01, True))
value_2 = data_bind_group[['DY']].apply(lambda x: col_clean(x, 0.01, False))

z_value = value_1.merge(value_2, on=['종목코드', 'SEC_NM_KOR'
                                     ]).sum(axis=1,
                                            skipna=False).to_frame('z_value')
data_bind = data_bind.merge(z_value, how='left', on=['종목코드', 'SEC_NM_KOR'])

print(data_bind.round(4).head())

z_momentum = data_bind_group[[
    '12M', 'K_ratio'
]].apply(lambda x: col_clean(x, 0.01, False)).sum(
    axis=1, skipna=False).to_frame('z_momentum')
data_bind = data_bind.merge(z_momentum, how='left', on=['종목코드', 'SEC_NM_KOR'])

print(data_bind.round(4).head())

data_z = data_bind[['z_quality', 'z_value', 'z_momentum']].copy()

data_bind_final = data_bind[['종목코드', 'z_quality', 'z_value', 'z_momentum'
                             ]].set_index('종목코드').apply(zscore,
                                                        nan_policy='omit')
data_bind_final.columns = ['quality', 'value', 'momentum']

wts = [0.3, 0.3, 0.3]
data_bind_final_sum = (data_bind_final * wts).sum(axis=1,
                                                  skipna=False).to_frame()
data_bind_final_sum.columns = ['qvm']
port_qvm = data_bind.merge(data_bind_final_sum, on='종목코드')
port_qvm['invest'] = np.where(port_qvm['qvm'].rank() <= 20, 'Y', 'N')

print(port_qvm[port_qvm['invest'] == 'Y'].round(4))

port_qvm[port_qvm['invest'] == 'Y'].round(4).to_excel('model.xlsx', index=False)