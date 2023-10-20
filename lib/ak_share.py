import akshare as ak
import pandas as pd
import configparser
import time
import datetime
import common
import os
import one_data
import utils

def get_off_codes():
  codes = []
  # 深证
  stock_staq_net_stop_df = ak.stock_info_sz_delist(symbol="终止上市公司")
  codes = codes + stock_staq_net_stop_df['证券代码'].to_list()

  # 
  stock_staq_net_stop_df = ak.stock_staq_net_stop()
  codes = codes + stock_staq_net_stop_df['代码'].to_list()

  stock_info_sh_delist_df = ak.stock_info_sh_delist()
  codes = codes + stock_info_sh_delist_df['公司代码'].to_list()

  codes = [x for x in codes if not str(x).startswith("200")]

  return codes
