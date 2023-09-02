import pandas as pd
import requests
code = '005930'
URL = f"https://finance.naver.com/item/main.nhn?code={code}"
r = requests.get(URL)
df = pd.read_html(r.text)[3]
df.set_index(df.columns[0],inplace=True)
df.index.rename('주요재무정보', inplace=True)
df.columns = df.columns.droplevel(2)
annual_date = pd.DataFrame(df).xs('최근 연간 실적',axis=1)
quater_date = pd.DataFrame(df).xs('최근 분기 실적',axis=1)


import requests
from bs4 import BeautifulSoup

def get_stock_price(stock_code):
    url = f"https://finance.naver.com/item/main.nhn?code={stock_code}"
    response = requests.get(url)

    if response.status_code != 200:
        print("Failed to fetch the webpage.")
        return None

    soup = BeautifulSoup(response.content, "html.parser")
    price_area = soup.find("p", {"class": "no_today"})
    if not price_area:
        print("Failed to find the price data.")
        return None

    price = price_area.find("span", {"class": "blind"}).text
    return price


# 삼성전자 주가 정보를 가져옵니다 (삼성전자의 종목 코드: 005930)
stock_code = "005930"
price = get_stock_price(stock_code)
if price:
    print(f"The current stock price of {stock_code} is: {price}")