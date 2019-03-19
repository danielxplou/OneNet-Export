# OneNet-Export
Export device and datapoint from CMCC's OneNet IOT platform.Coding by Python

v0.1 导出devices和datapoints到excel

v0.2 修改内容：
1、导出的数据写入mysql 数据库
2、每个小时自动导出一次数据，保持数据库中数据更新

v0.3 修改内容：
1、修改成多线程方式，每50个设备起一个线程从onenet导出数据insert到mysql数据表中
