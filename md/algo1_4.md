# algo1_4: free_market_cap > 150 p_change 3~5 turnover 5~15 next_open -2~-1, high > next_open + N 每日每只股票1万，限购前3， 持有3天。


# 分析开盘涨幅区间。结论：【暂无合适策略】

## next_up_p_change 1.0

   year open_p_change  next_up_p_change  trade_count  total_price_change  price_change_per_trade  success_rate
0   2020     -2.0~-1.9               1.0           10              5550.0                  555.00         70.00
1   2020     -1.9~-1.8               1.0            8              -132.0                  -16.50         62.50
2   2020     -1.8~-1.7               1.0            9              5132.0                  570.22         66.67
3   2020     -1.7~-1.6               1.0            9               740.0                   82.22         55.56
4   2020     -1.6~-1.5               1.0           16             -4286.0                 -267.88         25.00
5   2020     -1.5~-1.4               1.0           11               136.0                   12.36         54.55
6   2020     -1.4~-1.3               1.0           11             -1279.0                 -116.27         36.36
7   2020     -1.3~-1.2               1.0           15              1500.0                  100.00         46.67
8   2020     -1.2~-1.1               1.0           22              4372.0                  198.73         54.55
9   2020     -1.1~-1.0               1.0           20              4878.0                  243.90         65.00

10  2021     -2.0~-1.9               1.0           15             -2724.0                 -181.60         33.33
11  2021     -1.9~-1.8               1.0           12              2669.0                  222.42         58.33
12  2021     -1.8~-1.7               1.0            8             -1375.0                 -171.88         62.50
13  2021     -1.7~-1.6               1.0           14               936.0                   66.86         42.86
14  2021     -1.6~-1.5               1.0           16              1522.0                   95.12         56.25
15  2021     -1.5~-1.4               1.0           13              5921.0                  455.46         69.23
16  2021     -1.4~-1.3               1.0           16              4406.0                  275.38         68.75
17  2021     -1.3~-1.2               1.0           23             -7237.0                 -314.65         34.78
18  2021     -1.2~-1.1               1.0           19              7837.0                  412.47         63.16
19  2021     -1.1~-1.0               1.0           28              6904.0                  246.57         64.29

20  2022     -2.0~-1.9               1.0           11               141.0                   12.82         45.45
21  2022     -1.9~-1.8               1.0            6              -348.0                  -58.00         33.33
22  2022     -1.8~-1.7               1.0           12             -4657.0                 -388.08         16.67
23  2022     -1.7~-1.6               1.0           10              -978.0                  -97.80         40.00
24  2022     -1.6~-1.5               1.0           13              1036.0                   79.69         53.85
25  2022     -1.5~-1.4               1.0           10               347.0                   34.70         40.00
26  2022     -1.4~-1.3               1.0            9              -942.0                 -104.67         44.44
27  2022     -1.3~-1.2               1.0            9              1860.0                  206.67         66.67
28  2022     -1.2~-1.1               1.0            9                -3.0                   -0.33         55.56
29  2022     -1.1~-1.0               1.0           10             -1395.0                 -139.50         50.00

30  2023     -2.0~-1.9               1.0            8             -1922.0                 -240.25         50.00
31  2023     -1.9~-1.8               1.0           10                50.0                    5.00         50.00
32  2023     -1.8~-1.7               1.0            5               255.0                   51.00         20.00
33  2023     -1.7~-1.6               1.0            7             -3124.0                 -446.29         28.57
34  2023     -1.6~-1.5               1.0           17              2921.0                  171.82         47.06
35  2023     -1.5~-1.4               1.0           10              -307.0                  -30.70         60.00
36  2023     -1.4~-1.3               1.0           12             -1783.0                 -148.58         41.67
37  2023     -1.3~-1.2               1.0           16              1023.0                   63.94         50.00
38  2023     -1.2~-1.1               1.0           19              -501.0                  -26.37         36.84
39  2023     -1.1~-1.0               1.0           18             -2731.0                 -151.72         38.89

40  2024     -2.0~-1.9               1.0           11              2846.0                  258.73         63.64
41  2024     -1.9~-1.8               1.0           11              1711.0                  155.55         45.45
42  2024     -1.8~-1.7               1.0            6               245.0                   40.83         50.00
43  2024     -1.7~-1.6               1.0            9               328.0                   36.44         55.56
44  2024     -1.6~-1.5               1.0           14              2427.0                  173.36         64.29
45  2024     -1.5~-1.4               1.0            8              -374.0                  -46.75         25.00
46  2024     -1.4~-1.3               1.0           12             -3975.0                 -331.25         25.00
47  2024     -1.3~-1.2               1.0           15             -1117.0                  -74.47         33.33
48  2024     -1.2~-1.1               1.0           17             -1159.0                  -68.18         41.18
49  2024     -1.1~-1.0               1.0           11              2085.0                  189.55         63.64


## open_p_change -2.0~-1.5   next_up_p_change 2.4~3.8 [采用3.0 收益率勉强， 待优化]

    year open_p_change  next_up_p_change  trade_count  total_price_change  price_change_per_trade  success_rate
0   2020     -2.0~-1.5               2.4           32              1076.0                   33.62         40.62
1   2020     -2.0~-1.5               2.6           32              1071.0                   33.47         40.62
2   2020     -2.0~-1.5               2.8           31               329.0                   10.61         38.71
3   2020     -2.0~-1.5               3.0           30               402.0                   13.40         40.00
4   2020     -2.0~-1.5               3.2           29              -372.0                  -12.83         34.48
5   2020     -2.0~-1.5               3.4           28             -1015.0                  -36.25         35.71
6   2020     -2.0~-1.5               3.6           25             -1546.0                  -61.84         36.00
7   2020     -2.0~-1.5               3.8           25             -2126.0                  -85.04         36.00

8   2021     -2.0~-1.5               2.4           43               102.0                    2.37         48.84
9   2021     -2.0~-1.5               2.6           42              -397.0                   -9.45         47.62
10  2021     -2.0~-1.5               2.8           40               -95.0                   -2.38         50.00
11  2021     -2.0~-1.5               3.0           37              4658.0                  125.89         56.76
12  2021     -2.0~-1.5               3.2           37              3838.0                  103.73         56.76
13  2021     -2.0~-1.5               3.4           36              3645.0                  101.25         58.33
14  2021     -2.0~-1.5               3.6           35              4299.0                  122.83         60.00
15  2021     -2.0~-1.5               3.8           35              3559.0                  101.69         60.00

16  2022     -2.0~-1.5               2.4           30             -2608.0                  -86.93         43.33
17  2022     -2.0~-1.5               2.6           30             -3763.0                 -125.43         40.00
18  2022     -2.0~-1.5               2.8           28             -4652.0                 -166.14         39.29
19  2022     -2.0~-1.5               3.0           27             -3979.0                 -147.37         40.74
20  2022     -2.0~-1.5               3.2           23             -2949.0                 -128.22         47.83
21  2022     -2.0~-1.5               3.4           23             -3302.0                 -143.57         47.83
22  2022     -2.0~-1.5               3.6           21             -2308.0                 -109.90         52.38
23  2022     -2.0~-1.5               3.8           21             -2808.0                 -133.71         42.86

24  2023     -2.0~-1.5               2.4           29              3593.0                  123.90         48.28
25  2023     -2.0~-1.5               2.6           28              4778.0                  170.64         53.57
26  2023     -2.0~-1.5               2.8           26              5397.0                  207.58         57.69
27  2023     -2.0~-1.5               3.0           24              4748.0                  197.83         54.17
28  2023     -2.0~-1.5               3.2           24              4268.0                  177.83         54.17
29  2023     -2.0~-1.5               3.4           22              3815.0                  173.41         54.55
30  2023     -2.0~-1.5               3.6           21              2462.0                  117.24         52.38
31  2023     -2.0~-1.5               3.8           21              2042.0                   97.24         52.38

32  2024     -2.0~-1.5               2.4           34              3462.0                  101.82         58.82
33  2024     -2.0~-1.5               2.6           31              2742.0                   88.45         54.84
34  2024     -2.0~-1.5               2.8           29              3256.0                  112.28         51.72
35  2024     -2.0~-1.5               3.0           26              3697.0                  142.19         53.85
36  2024     -2.0~-1.5               3.2           24              2308.0                   96.17         54.17
37  2024     -2.0~-1.5               3.4           23              1891.0                   82.22         56.52
38  2024     -2.0~-1.5               3.6           21              1308.0                   62.29         52.38
39  2024     -2.0~-1.5               3.8           21               848.0                   40.38         47.62



## open_p_change -1.5~-1.0   next_up_p_change 2.4~3.8 [采用2.4 收益率不稳定，待优化]

   year open_p_change  next_up_p_change  trade_count  total_price_change  price_change_per_trade  success_rate
0   2020     -1.5~-1.0               2.4           45              4780.0                  106.22         51.11
1   2020     -1.5~-1.0               2.6           44              4621.0                  105.02         54.55
2   2020     -1.5~-1.0               2.8           44              4058.0                   92.23         54.55
3   2020     -1.5~-1.0               3.0           42              3717.0                   88.50         52.38
4   2020     -1.5~-1.0               3.2           39              2635.0                   67.56         53.85
5   2020     -1.5~-1.0               3.4           35              3306.0                   94.46         57.14
6   2020     -1.5~-1.0               3.6           34              2969.0                   87.32         55.88
7   2020     -1.5~-1.0               3.8           33              3107.0                   94.15         54.55

8   2021     -1.5~-1.0               2.4           61              9634.0                  157.93         50.82
9   2021     -1.5~-1.0               2.6           59              7107.0                  120.46         49.15
10  2021     -1.5~-1.0               2.8           57              3779.0                   66.30         49.12
11  2021     -1.5~-1.0               3.0           55              4294.0                   78.07         52.73
12  2021     -1.5~-1.0               3.2           52              5227.0                  100.52         55.77
13  2021     -1.5~-1.0               3.4           49              3556.0                   72.57         57.14
14  2021     -1.5~-1.0               3.6           48              2348.0                   48.92         56.25
15  2021     -1.5~-1.0               3.8           48              2116.0                   44.08         54.17

16  2022     -1.5~-1.0               2.4           28             -2753.0                  -98.32         35.71
17  2022     -1.5~-1.0               2.6           26             -2215.0                  -85.19         38.46
18  2022     -1.5~-1.0               2.8           26             -2835.0                 -109.04         38.46
19  2022     -1.5~-1.0               3.0           25             -2345.0                  -93.80         40.00
20  2022     -1.5~-1.0               3.2           22             -2616.0                 -118.91         40.91
21  2022     -1.5~-1.0               3.4           20              -725.0                  -36.25         50.00
22  2022     -1.5~-1.0               3.6           20             -1165.0                  -58.25         50.00
23  2022     -1.5~-1.0               3.8           19             -1112.0                  -58.53         47.37

24  2023     -1.5~-1.0               2.4           44             -8302.0                 -188.68         29.55
25  2023     -1.5~-1.0               2.6           43             -8390.0                 -195.12         30.23
26  2023     -1.5~-1.0               2.8           42             -8976.0                 -213.71         30.95
27  2023     -1.5~-1.0               3.0           38             -9083.0                 -239.03         31.58
28  2023     -1.5~-1.0               3.2           38             -9943.0                 -261.66         28.95
29  2023     -1.5~-1.0               3.4           36             -9341.0                 -259.47         30.56
30  2023     -1.5~-1.0               3.6           34             -8053.0                 -236.85         32.35
31  2023     -1.5~-1.0               3.8           31             -5145.0                 -165.97         32.26

32  2024     -1.5~-1.0               2.4           34             -2458.0                  -72.29         38.24
33  2024     -1.5~-1.0               2.6           31             -3946.0                 -127.29         38.71
34  2024     -1.5~-1.0               2.8           31             -4137.0                 -133.45         38.71
35  2024     -1.5~-1.0               3.0           27             -1592.0                  -58.96         44.44
36  2024     -1.5~-1.0               3.2           27             -2252.0                  -83.41         40.74
37  2024     -1.5~-1.0               3.4           25             -2799.0                 -111.96         40.00
38  2024     -1.5~-1.0               3.6           23             -2371.0                 -103.09         39.13
39  2024     -1.5~-1.0               3.8           23             -2911.0                 -126.57         39.13