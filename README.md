# spark-tock

## 背景

此项目可以查看截止2021-06-30号，各基金公司持有的股票信息。本项目使用spark作为分析引擎，基于spark-rest开发

## 内容

只要功能介绍

### 查看基金公司信息

使用Test_Fund类的test_GetAllFund方法，将获取数据以parquet方式存在local，运行结果如下：
+-------+-------------+-------------+--------+--------------------+
|     id|   reduceName|  chineseName|typeName|          fullPYName|
+-------+-------------+-------------+--------+--------------------+
|﻿000001|       HXCZHH|       华夏成长混合|  混合型-偏股|HUAXIACHENGZHANGH...|
| 000002|       HXCZHH|   华夏成长混合(后端)|  混合型-偏股|HUAXIACHENGZHANGH...|
| 000003|     ZHKZZZQA|     中海可转债债券A| 债券型-可转债|ZHONGHAIKEZHUANZH...|
| 000004|     ZHKZZZQC|     中海可转债债券C| 债券型-可转债|ZHONGHAIKEZHUANZH...|
| 000005|   JSZQXYDQZQ|   嘉实增强信用定期债券|  债券型-长债|JIASHIZENGQIANGXI...|
| 000006|  XBLDLHCZHHA|  西部利得量化成长混合A|  混合型-偏股|XIBULIDELIANGHUAC...|
| 000008|JSZZ500ETFLJA|嘉实中证500ETF联接A|  指数型-股票|JIASHIZHONGZHENG5...|
| 000009|   YFDTTLCHBA|   易方达天天理财货币A|     货币型|YIFANGDATIANTIANL...|
| 000010|   YFDTTLCHBB|   易方达天天理财货币B|     货币型|YIFANGDATIANTIANL...|
| 000011|    HXDPJXHHA|    华夏大盘精选混合A|  混合型-偏股|HUAXIADAPANJINGXU...|
| 000012|    HXDPJXHHA|华夏大盘精选混合A(后端)|  混合型-偏股|HUAXIADAPANJINGXU...|
| 000013|   YFDTTLCHBR|   易方达天天理财货币R|     货币型|YIFANGDATIANTIANL...|
| 000014|       HXJLZQ|       华夏聚利债券| 债券型-混合债|  HUAXIAJULIZHAIQUAN|
| 000015|      HXCZZQA|      华夏纯债债券A|  债券型-长债|HUAXIACHUNZHAIZHA...|
| 000016|      HXCZZQC|      华夏纯债债券C|  债券型-长债|HUAXIACHUNZHAIZHA...|
| 000017|      CTKCXHH|      财通可持续混合|  混合型-偏股| CAITONGKECHIXUHUNHE|
| 000020|   JSCCPZTZHH|   景顺长城品质投资混合|  混合型-偏股|JINGSHUNCHANGCHEN...|
| 000021|     HXYSZZHH|     华夏优势增长混合|  混合型-偏股|HUAXIAYOUSHIZENGZ...|
| 000024|    DMSLZQZQA|    大摩双利增强债券A|  债券型-长债|DAMOSHUANGLIZENGQ...|
| 000025|    DMSLZQZQC|    大摩双利增强债券C|  债券型-长债|DAMOSHUANGLIZENGQ...|
+-------+-------------+-------------+--------+--------------------+
only showing top 20 rows

root
 |-- id: string (nullable = true)
 |-- reduceName: string (nullable = true)
 |-- chineseName: string (nullable = true)
 |-- typeName: string (nullable = true)
 |-- fullPYName: string (nullable = true)

###  查看股票信息

 使用Test_Stock类的test_GetStockInfoByStockId方法，查看具体信息如下

 008584>>>>[]
 2021-06-30
 008585>>>>[GuPiaoInfo{jjId='008585', dateText='2021-06-30', code='300919', name='中伟股份', percent='0.02%', ownCount='0.06', ownValue='9.94'}, GuPiaoInfo{jjId='008585', dateText='2021-06-30', code='300925', name='法本信息', percent='0.00%', ownCount='0.04', ownValue='0.93'}, GuPiaoInfo{jjId='008585', dateText='2021-06-30', code='300926', name='博俊科技', percent='0.00%', ownCount='0.04', ownValue='0.80'}, GuPiaoInfo{jjId='008585', dateText='2021-06-30', code='300927', name='江天化学', percent='0.00%', ownCount='0.02', ownValue='0.65'}]
 2021-06-30
 008586>>>>[GuPiaoInfo{jjId='008586', dateText='2021-06-30', code='300919', name='中伟股份', percent='0.02%', ownCount='0.06', ownValue='9.94'}, GuPiaoInfo{jjId='008586', dateText='2021-06-30', code='300925', name='法本信息', percent='0.00%', ownCount='0.04', ownValue='0.93'}, GuPiaoInfo{jjId='008586', dateText='2021-06-30', code='300926', name='博俊科技', percent='0.00%', ownCount='0.04', ownValue='0.80'}, GuPiaoInfo{jjId='008586', dateText='2021-06-30', code='300927', name='江天化学', percent='0.00%', ownCount='0.02', ownValue='0.65'}]
 008587>>>>[]

###  查看伊利股份信息

 通过以上方法可以获取基金和股票的信息，使用Test_600887类，就可以统计以下信息

####  查看伊利股份信息

 +------+----------+------+----+--------+---------+-------+------+-----------------+
 |code  |dateText  |jjId  |name|ownCount|ownValue |percent|id    |chineseName      |
 +------+----------+------+----+--------+---------+-------+------+-----------------+
 |600887|2021-06-30|008770|伊利股份|14.88   |548.03   |0.51%  |008770|东方红安鑫甄选一年持有混合    |
 |600887|2021-06-30|008851|伊利股份|92.20   |3,395.73 |2.12%  |008851|景顺长城量化对冲策略三个月定开  |
 |600887|2021-06-30|008895|伊利股份|25.19   |927.75   |0.96%  |008895|申万菱信量化对冲策略混合     |
 |600887|2021-06-30|008901|伊利股份|344.31  |12,680.85|5.87%  |008901|富国内需增长混合A        |
 |600887|2021-06-30|008907|伊利股份|0.20    |7.37     |0.05%  |008907|汇添富中证国企一带一路ETF联接A|
 |600887|2021-06-30|008908|伊利股份|0.20    |7.37     |0.05%  |008908|汇添富中证国企一带一路ETF联接C|
 |600887|2021-06-30|008936|伊利股份|3.08    |113.44   |0.55%  |008936|中银产业债债券C         |
 |600887|2021-06-30|008975|伊利股份|2.92    |107.54   |0.28%  |008975|富国中证消费50ETF联接A   |
 |600887|2021-06-30|008976|伊利股份|2.92    |107.54   |0.28%  |008976|富国中证消费50ETF联接C   |
 |600887|2021-06-30|008990|伊利股份|24.07   |886.35   |0.41%  |008990|东方红匠心甄选一年持有混合    |
 |600887|2021-06-30|009029|伊利股份|410.36  |15,113.55|5.01%  |009029|工银高质量成长混合A       |
 |600887|2021-06-30|009030|伊利股份|410.36  |15,113.55|5.01%  |009030|工银高质量成长混合C       |
 |600887|2021-06-30|009071|伊利股份|5.00    |184.15   |0.73%  |009071|德邦安鑫混合A          |
 |600887|2021-06-30|009072|伊利股份|5.00    |184.15   |0.73%  |009072|德邦安鑫混合C          |
 |600887|2021-06-30|009076|伊利股份|1,100.35|40,525.91|3.00%  |009076|工银圆兴混合           |
 |600887|2021-06-30|009102|伊利股份|17.50   |644.53   |2.68%  |009102|鹏扬红利优选混合A        |
 |600887|2021-06-30|009103|伊利股份|17.50   |644.53   |2.68%  |009103|鹏扬红利优选混合C        |
 |600887|2021-06-30|009116|伊利股份|10.79   |397.40   |6.33%  |009116|东兴中证消费50A        |
 |600887|2021-06-30|009117|伊利股份|10.79   |397.40   |6.33%  |009117|东兴中证消费50C        |
 |600887|2021-06-30|009179|伊利股份|1.10    |40.59    |0.15%  |009179|嘉实中证主要消费ETF联接A   |
 +------+----------+------+----+--------+---------+-------+------+-----------------+
 only showing top 20 rows

 持有机构共:611家

 Process finished with exit code 0

**结论**：共有611基金持有伊利股份

####统计伊利股份信息，如下
+------------------+----------+-----------------+---------------+------------------+
|        avg(price)|max(price)|       min(price)|sum(n_ownValue)|   sum(n_ownCount)|
+------------------+----------+-----------------+---------------+------------------+
|36.842631996324535|      44.5|25.67984832069339|     3846884.52|104460.32999999999|
+------------------+----------+-----------------+---------------+------------------+

**结论**

1. 611家基金持有伊利平均成本36.84元
2. 611家基金最小成本25.6元， 最大成本44.5元
3. 611家持有104460.3万股，3846884.52万元人民币

### 统计

统计前20名公司，使用TestStatistics类按价值统计，如下：
+----+------------------+------------------+------------------+--------------------+------------------+----+
|name|        avg(price)|        max(price)|        min(price)|      sum_n_ownValue|   sum(n_ownCount)|  ct|
+----+------------------+------------------+------------------+--------------------+------------------+----+
|贵州茅台|2051.4960173301583|           2308.75| 729.9999999999999|2.5570052249999993E7|          12433.66|2530|
|宁德时代| 534.3275636242015|            547.65|174.34999999999997|       2.011243045E7|37608.229999999996|1901|
| 五粮液|297.78518924883343|             369.0|             221.0|1.8883246600000005E7| 63391.18000000001|1892|
|腾讯控股| 485.3443289267074|             515.5|297.83333333333337|1.5769682559999997E7|32451.950000000004| 844|
|药明康德|156.44518136584915|             219.0|             135.0|1.1966857390000004E7|          76494.34|1407|
|海康威视| 64.39972947325694|              65.0|              36.0|       1.178226081E7|182834.97999999995|1200|
|隆基股份| 88.59622801163103|             103.0|26.197802197802197|1.0645941189999996E7|119871.94000000002|1379|
|招商银行|54.049668338396074| 56.16666666666667|             29.09|   9723914.879999999|179196.63999999998|1678|
|迈瑞医疗| 480.0304120618171|             490.6| 468.8333333333333|          8847950.73|          18431.26| 963|
|美团-W| 266.4494202015394| 266.7262277951933|251.99999999999997|   7750061.470000001|29070.260000000002| 398|
|山西汾酒| 446.5803943603716|453.72222222222223|              60.0|   7421767.360000001|          16566.45| 478|
|泸州老窖|  234.450645279401|236.78124999999997|31.080000000000002|   7335744.339999999|31100.149999999994| 437|
|中国中免| 299.3785644472534| 316.4285714285714|              70.0|   7122294.480000001|23734.759999999995|1051|
|爱尔眼科| 70.98955864869023|  72.6842105263158|              69.0|          6026429.07| 84903.12000000001| 497|
|亿纬锂能|103.46697525696412|             105.6| 57.99999999999999|   5842968.479999999| 56222.82000000001| 659|
|东方财富|  32.6923437487123| 33.76923076923077| 13.54962707974756|   5729680.299999999|         174781.68| 982|
|洋河股份| 206.7242075558223|215.27272727272728|             128.0|   4911025.069999999|23702.760000000002| 167|
|立讯精密| 46.00380069211009|             46.95| 45.66666666666667|          4515967.33| 98173.14999999998| 630|
|智飞生物| 185.5404934612535|            187.28| 51.09740259740259|           3894115.8|20890.440000000002| 500|
|赣锋锂业|119.29396413785982|121.37719298245615|             94.25|  3883318.1099999994|32571.030000000002| 445|
+----+------------------+------------------+------------------+--------------------+------------------+----+
only showing top 20 rows