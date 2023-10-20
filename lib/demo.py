import pandas as pd
import datetime
import numpy as np
import akshare as ak
import tushare as ts
from matplotlib import pyplot as plt
from urllib.parse import urlencode
import stock

ps = pd.Series(['000001', '000002'])
dates = pd.date_range(start='20120501', periods=32)
df = pd.DataFrame(np.random.randn(7,4), index = list('1234567'), columns = list('ABCD'))
# df = pd.DataFrame(np.random.randn(7,4), index = dates, columns = list('ABCD'))

df2 = pd.DataFrame({'A': 1.,
					'B': pd.Timestamp('20160101'),
					'C': pd.Series(1, index = list(range(4)), dtype = 'float32'),
					'D': np.array([3] * 4, dtype = 'int32'),
					'E': pd.Categorical(['test', 'train', 'test', 'train']),
					'F': 'foo'})
print(df2['A'][3])

stock.update_stock_basic_append()
# 
exit()
# df.drop(df[0:1].index.get_values()[0])

df.head()
df.tail(3)

df.index
df.index.size #row number
df.columns
df.columns.size #column number
df.values
df.describe()
# df.T
df.sort_index(axis=1, ascending=False)  #????
df.sort_values(by='B', ascending=False)

df['A'] #Series
df.A #Series
df[0:3] #DataFrame
df['2010-01-01':'201001-03'] #DataFrame
df.loc[dates[0]] #Series
df.loc[:,['A','B']] #DataFrame
df.loc['20100101':'20100103',['A','B']] #DataFrame
df.loc['20100101','A':'C'] #Series
df.loc['20100101','A'] #data
df.at[dates[0],'A'] #equal before
df.iloc[3] #Series
df.iloc[3:5,0:2] #DataFrame
df.iloc[[1,2,4],[0,2]] #DataFrame
df.iloc[1:3,:] #DataFrame
df.iloc[1,1] #data
df[df.A > 0]
df[df > 0]
df2 = df.copy()
df2['E'] = ['one', 'two', 'three', 'four', 'five', 'six', 'seven']
df2[df2['E'].isin(['two','four'])]

df1 = df.reindex(index=dates[0:4],columns=list(df.columns) + ['E'])
df1.loc[dates[0]:dates[1], 'E'] = 1
df1.dropna(how='any')
df1.fillna(value=5)
pd.isnull(df1)

df.mean() #column average
df.mean(1) #index average

df.apply(np.cumsum)

s = np.random.randint(0,7,size=20)
s = pd.Series(s)
s.value_counts()

df = pd.Series(np.random.randn(1000), index=pd.date_range('20100101', periods=1000))
df = df.cumsum()
plt.figure("test1")
plt.plot(df)
plt.show()

plt.figure("test2")
df = ts.get_hist_data('000001','2016-05-01','2016-08-26')
df = pd.Series(df.loc[:,'close'].values, index=pd.date_range(end='20160826',periods=len(df)))
# plt.plot(df)
# plt.show()