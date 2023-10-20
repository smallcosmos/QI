# 
import sys

if not "../lib/" in sys.path:
	sys.path.append("../lib/")

import filters as ft
import common
import pandas as pd

__limit__ = 100

utilityCode = ft.filter_utilities()
financeCode = ft.filter_financials()
lossCode = ft.filter_loss()
gemCode = ft.filter_gem()

exclude = common.unionList(lists = [utilityCode, financeCode, lossCode, gemCode])
peCode = ft.sort_pe()
roeCode = ft.sort_roe()

codes = common.unionList(peCode, roeCode)
result = pd.Series(0, index=codes)
for code in codes:
	index = 0
	if code in peCode:
		index += peCode.index(code)
	else:
		index += __limit__
	if code in roeCode:
		index += roeCode.index(code)
	else:
		index += __limit__
	result[code] = index

result.sort_values(ascending = True)
result = list(result.index)
result = common.diffList(result, exclude)[0:30]

print(result)