'''
source:
https://medium.com/@chris_42047/how-to-download-historic-1-minute-price-data-for-crypto-for-free-python-9583731fc8
'''

import os
import numpy as np
import pandas as pd
import datetime
import time
from datetime import date
import pandas_ta as ta
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from yahoo_fin import stock_info as si
import datetime
import yfinance as yf
from random import randint
from time import sleep


def load_historic_data(symbol, start_date_str, today_date_str, period, interval, prepost):
    try:
        df = yf.download(symbol, start=start_date_str, end=today_date_str, period=period, interval=interval, prepost=prepost)
        #  Add symbol
        df["Symbol"] = symbol
        return df
    except:
        print('Error loading stock data for ' + symbols)
        return None

pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)
# valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
# fetch data by interval (including intraday if period < 60 days)
# valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
period = '1d'
interval = '1d'
prepost = True
today = datetime.date.today()
today_date_str = today.strftime("%Y-%m-%d")
#  NOTE: 7 days is the max allowed
days = datetime.timedelta(7)
start_date = today - days
start_date_str = datetime.datetime.strftime(start_date, "%Y-%m-%d")
#  Coins to download
# symbols = ['BTC-USD','ETH-USD','BCH-USD','LTC-USD','BNB-USD','BAT-USD', 'XLM-USD','DOGE-USD','DOGE-USD','COMP-USD','ALGO-USD','OMG-USD']
symbols = ['BTC-USD']

df = load_historic_data('BTC-USD', start_date_str, today_date_str, period, interval, prepost)
df = yf.download(['IOTA-USD'], start="2015-01-01", end="2024-03-06", period=period, interval=interval, prepost=prepost)
#  Fetch data for coin symbols
# for symbol in symbols:
#     print(f"Loading data for {symbol}")
#     df = load_historic_data(symbol, start_date_str, today_date_str, period, interval, prepost)
#     #  Save df
#     # file_name = f"{today_date_str}_{symbol}_{period}_{interval}.csv"
#     # df.to_csv(f"C:\\dev\\trading\\tradesystem1\\data\\crypto\\{file_name}")
#
#     #  Avoid DOS issues
#     sleep(randint(0,3))
