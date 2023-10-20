# coding=utf-8
import numpy as np
import pandas as pd
import utils
import time
from pandas import DataFrame
import schedule
import datetime

def algo():
  stock_basic = utils.get_stock_codes()
  for code in stock_basic:
    df = utils.get_stock_p_change_after_sequent_down(code, sequent_down_days=3)
    print(code, df)
    time.sleep(5)