#-*- coding: UTF-8 -*-

import math
import time
import json
import urllib
import requests
import datetime
import configparser
import datetime
import os
import pandas as pd
import numpy as np
from urllib.parse import urlencode

config = configparser.ConfigParser()
configFile = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config.ini')

config.read(configFile)


def get_stock300():
    """
    东方财富网-沪深300
    https://data.eastmoney.com/other/index/
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "pageSize": "5000",
        "pageNumber": "1",
        "columns": ','.join(["SECURITY_CODE"]),
        "source": "WEB",
        "client": "WEB",
        "reportName": "RPT_INDEX_TS_COMPONENT",
        "filter": "(TYPE=1)"
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    if not data_json["result"]["data"]:
        return pd.DataFrame()
    
    temp_df = pd.DataFrame(data_json["result"]["data"])
    return temp_df


# get_stock_valuation('2023-09-06')
def get_stock_valuation(date: str):
    """
    东方财富网-沪深300
    https://data.eastmoney.com/gzfx/list.html?date=2023-09-26
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "SECURITY_CODE",
        "sortTypes": "1",
        "pageSize": "10000",
        "pageNumber": "1",
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "reportName": "RPT_VALUEANALYSIS_DET",
        "filter": f"(TRADE_DATE='{date}')"
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    if not data_json["result"] or not data_json["result"]["data"]:
        return pd.DataFrame()
    
    temp_df = pd.DataFrame(data_json["result"]["data"])
    return temp_df


# 获取股票指数列表
def get_stock_index():
    """
    东方财富网-指数
    https://quote.eastmoney.com/center/hszs.html
    """
    url = "https://78.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "200",
        "fs": "b:MK0010",
        "fields": "f12,f13,f14"
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    if not data_json["data"] or not data_json["data"]["diff"]:
        return pd.DataFrame()
    
    temp_df = pd.DataFrame(data_json["data"]["diff"])
    temp_df = temp_df.transpose()
    return temp_df


# 获取股票指数历史数据
def get_stock_index_hist(sec_id: str, start_date: str, end_date: str):
    """
    东方财富网-指数
    https://quote.eastmoney.com/zs000001.html#fullScreenChart
    """
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": sec_id,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": 101,
        "fqt": 1,
        "end": "20500101",
        "lmt": 10000
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    if not data_json["data"] or not data_json["data"]["klines"]:
        return pd.DataFrame()
    
    klines = data_json["data"]["klines"]
    split_data = [x.split(',') for x in klines]
    temp_df = pd.DataFrame(split_data, columns=["date", "open", "close", "high", "low", "volume", "amount", "amplitude", "p_change", "price_change", "turnover"])
    start_date = pd.to_datetime(start_date).date().strftime('%Y-%m-%d')
    end_date = pd.to_datetime(end_date).date().strftime('%Y-%m-%d')
    filtered_df = temp_df[(temp_df['date'] >= start_date) & (temp_df['date'] <= end_date)]
    return filtered_df


# 获取股票指数当日行情
def get_stock_index_today():
    """
    东方财富网-指数
    https://quote.eastmoney.com/center/hszs.html
    """
    url = "https://97.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": 1,
        "pz": 50,
        "po": 1,
        "np": 1,
        "fltt": 2,
        "invt": 2,
        "fs": "b:MK0010",
        "fields": "f2,f3,f4,f5,f6,f7,f8,f12,f15,f16,f17",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    if not data_json["data"] or not data_json["data"]["diff"]:
        return pd.DataFrame()
    
    temp_df = pd.DataFrame(data_json["data"]["diff"])
    temp_df = temp_df.rename(columns={'f2': 'close', 'f3': 'p_change', 'f4': 'price_change', 'f5': 'volume', 'f6': 'amount', 'f7': 'amplitude', 'f8': 'turnover', 'f12': 'code', 'f15': 'high', 'f16': 'low', 'f17': 'open', })
    temp_df['date'] = pd.to_datetime(datetime.date.today())
    return temp_df

# 获取今日实时行情
def get_today_stock_hist():
    """
    东方财富网-a股
    https://quote.eastmoney.com/center/gridlist.html#hs_a_board
    """
    url = "https://92.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": 1,
        "pz": 10000,
        "po": 0,
        "np": 1,
        "fltt": 2,
        "invt": 2,
        "fid": 'f3',
        "fs": 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048',
        "fields": "f2,f3,f4,f5,f6,f7,f8,f12,f15,f16,f17",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    if not data_json["data"] or not data_json["data"]["diff"]:
        return pd.DataFrame()
    
    temp_df = pd.DataFrame(data_json["data"]["diff"])
    temp_df = temp_df.rename(columns={'f2': 'close', 'f3': 'p_change', 'f4': 'price_change', 'f5': 'volume', 'f6': 'amount', 'f7': 'amplitude', 'f8': 'turnover', 'f12': 'code', 'f15': 'high', 'f16': 'low', 'f17': 'open', })
    temp_df['date'] = pd.to_datetime(datetime.date.today())
    temp_df = temp_df.replace('-', float('nan'))
    columns = ['p_change']
    for column in columns:
        temp_df[column] = temp_df[column].astype(np.float64)

    return temp_df

def get_stock_a():
    df = get_today_stock_hist()
    return df['code']

# 获取两网退市
def get_stock_a_off():
    """
    东方财富网-两网及退市
    https://quote.eastmoney.com/center/gridlist.html#hs_a_board
    """
    url = "https://18.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": 1,
        "pz": 10000,
        "po": 0,
        "np": 1,
        "fltt": 2,
        "invt": 2,
        "fid": 'f3',
        "fs": 'm:0+s:3',
        "fields": "f12",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    if not data_json["data"] or not data_json["data"]["diff"]:
        return pd.Series()
    
    temp_df = pd.DataFrame(data_json["data"]["diff"])
    temp_df = temp_df.rename(columns={'f12': 'code'})

    return temp_df['code']


def get_stock_basic(codes):
    """
    东方财富网-公司概况
    https://emweb.securities.eastmoney.com/pc_hsf10/pages/index.html?type=web&code=SH603013&color=b#/gsgk
    """
    url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
    chunk_size = 300
    chunk_number = math.ceil(len(codes) / chunk_size)
    columns = ['SECURITY_CODE','ORG_NAME_EN','ORG_NAME','SECURITY_NAME_ABBR','INDUSTRYCSRC1','TRADE_MARKETT','REG_ADDRESS']
    df = pd.DataFrame(columns=columns)
    for index in range(0, chunk_number):
        sub_codes = codes[index*chunk_size:(index+1)*chunk_size]

        code_str = '"' + '","'.join(sub_codes) + '"'
        params = {
            "reportName": "RPT_F10_BASIC_ORGINFO",
            "columns": ','.join(columns),
            "pageNumber": 1,
            "pageSize": 10000,
            "filter": f'(SECURITY_CODE in ({code_str}))',
            "source": "HSF10",
        }
        r = requests.get(url, params=params)
        data_json = r.json()

        if not data_json["result"] or not data_json["result"]["data"]:
            temp_df = pd.DataFrame()
            continue
        
        temp_df = pd.DataFrame(data_json["result"]["data"])
        df = pd.concat([df, temp_df])
        time.sleep(0.5)
    
    df = df.reset_index(drop=True)
    return df
