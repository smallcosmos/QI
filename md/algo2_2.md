# algo2_2: free_market_cap 20~40  p_change 3~5 turnover 5~15 next_open 0.5~1, high > next_open + 1.6 每日每只股票1万，限购前3， 持有3天。

# 分析上涨触发买入， 结论使用 1.6

    year open_p_change  next_up_p_change  trade_count  total_price_change  price_change_per_trade  success_rate
0   2020       0.5~1.0               1.0           81              3360.0                   41.48         55.56
1   2020       0.5~1.0               1.2           76              2816.0                   37.05         55.26
2   2020       0.5~1.0               1.4           74              1633.0                   22.07         52.70
3   2020       0.5~1.0               1.6           72              3487.0                   48.43         52.78
4   2020       0.5~1.0               1.8           69              3385.0                   49.06         49.28

5   2021       0.5~1.0               1.0           64              7186.0                  112.28         45.31
6   2021       0.5~1.0               1.2           61              4177.0                   68.48         45.90
7   2021       0.5~1.0               1.4           59              4133.0                   70.05         44.07
8   2021       0.5~1.0               1.6           56              2998.0                   53.54         42.86
9   2021       0.5~1.0               1.8           55              2495.0                   45.36         43.64

10  2022       0.5~1.0               1.0           83             11583.0                  139.55         40.96
11  2022       0.5~1.0               1.2           79              8187.0                  103.63         39.24
12  2022       0.5~1.0               1.4           76             10610.0                  139.61         46.05
13  2022       0.5~1.0               1.6           73             10655.0                  145.96         47.95
14  2022       0.5~1.0               1.8           70             10042.0                  143.46         45.71

15  2023       0.5~1.0               1.0           50              3024.0                   60.48         40.00
16  2023       0.5~1.0               1.2           49              2221.0                   45.33         38.78
17  2023       0.5~1.0               1.4           47              1772.0                   37.70         38.30
18  2023       0.5~1.0               1.6           44              1423.0                   32.34         38.64
19  2023       0.5~1.0               1.8           40              3156.0                   78.90         40.00

20  2024       0.5~1.0               1.0           83             30299.0                  365.05         51.81
21  2024       0.5~1.0               1.2           80             30247.0                  378.09         55.00
22  2024       0.5~1.0               1.4           78             30153.0                  386.58         53.85
23  2024       0.5~1.0               1.6           75             31303.0                  417.37         52.00
24  2024       0.5~1.0               1.8           74             29408.0                  397.41         51.35