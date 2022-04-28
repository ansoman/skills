#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
"""
       @Time    : 2021/11/17 9:29
       @Author  : fab
       @File    : sparkWriteHbase.py
       @Email   : xxxx@etransfar.com
"""
from pyspark.sql import SparkSession
import pandas as pd
from recommendScore import recommendSys
from configs import database
import pymysql
from logger import log
from datetime import datetime
from pandas.testing import assert_frame_equal
# from pyspark.sql.functions import lit, concat
import os
import sys
import time
from pyspark.sql.functions import udf, col,lit,concat,collect_list
from sqlalchemy import create_engine
import logging


# logging.basicConfig(level=logging.DEBUG,#控制台打印的日志级别
#                     filename='new.log',
#                     filemode='a',##模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
#                     #a是追加模式，默认如果不写的话，就是追加模式
#                     format=
#                     '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
#                     #日志格式
#                     )


"""
                  -- --
  Write recommend result into hbase by spark.
"""


def select(sql, columns, param):
    """
      select data from mysql according to given sql.
    Input
    ------
      sql         :str;
      columns     :list;

    Output
    ------
      return      :pandas.DataFrame.
    """
    # param = database.mysql
    conn = pymysql.connect(host=param["host"],
                           port=param["port"],
                           user=param["user"],
                           password=param["password"],
                           database=param["database"])
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        data = cursor.fetchall()
        data = pd.DataFrame(data, columns=columns)
    except BaseException as e:
        print(str(e))
        data = pd.DataFrame()
    finally:
        cursor.close()
        conn.close()

    return data

def selectOrder():
    """
      select timing orders from order system.
    Input
    ------

    Output
    ------
      return      :pandas.DataFrame;
    """
    param = database.trade
    sql = """
          select
            tpPublishedOrderId,      -- 货源订单id
            orderNo,                 -- 订单编号  1
            shipperPartyId,          -- 货主会员id
            -- shipperPartyName,        -- 货主会员名称
            -- shipperRealName,         -- 货主真实名称
            shipperCompanyName,      -- 货主企业名称 1
            totalFee,                -- 总运费 1
            goodsType,               -- 货物类型;自定义输入 1
            goodsName,              -- 货物行业类型
            weight,                  -- 重量 1
            volume,                  -- 体积 1
            vehicleTypes,            -- 车型,多个用逗号隔开 1
            vehicleLengths,          -- 车长,多个用逗号隔开 1
            expireDate,              -- 失效时间
            loadingDate,             -- 装货时间 1
            offDate,                 -- 下架时间
            linkMobile,              -- 联系电话 1
            linkMan,                 -- 联系人姓名 1
            startProvince,           -- 出发省 1
            startCity,               -- 出发城市 1
            startTown,               -- 出发区县 1
            -- startDetail,             -- 出发详细地址
            endProvince,             -- 目的地省 1
            endCity,                 -- 目的地市 1
            endTown,                 -- 目的地区县 1
            -- endDetail,               -- 目的地详细地址
            -- evaluateDistance,        -- 预估距离
            publishType,             -- 发布类型; 1:全平台;2:私有运力
            flag,                    -- 订单状态 -1已下架  10已发布 20已指派 1
            -- isDelete,                -- 是否删除;0:未删;1:已删
            inputDate                -- 记录插入时间  1
          from TpPublishedOrder
          where flag=10 and isDelete=0
          order by inputDate desc
          -- limit 300
          """
    columns = ["tpPublishedOrderId", "orderNo", "shipperPartyId", "shipperCompanyName", "totalFee", "goodsType",
               "goodsName", "weight", "volume", "vehicleTypes", "vehicleLengths", "expireDate", "loadingDate",
               "offDate",
               "linkMobile", "linkMan", "startProvince", "startCity", "startTown", "endProvince",
               "endCity", "endTown", "publishType", "flag", "inputDate"]
    order = select(sql, columns, param)

    return order

def checKIfChange():
    data = pd.read_csv("/home/admin/zr/lytrecommendsystem_spark/model/orders.csv")
    data2 = selectOrder()

    if data2.shape[0] == 0:
        return 1
    else:
        setHis = set(data['orderNo'].values)
        setCur = set(data2['orderNo'].values)
        print(setHis == setCur)
        if setHis == setCur:
            return 1
        else:
            return 0

class sparkWriteHbase:
    def __init__(self):
        self.warehouse_location = "/user/hive/warehouse"
        self.data_source_format = 'org.apache.hadoop.hbase.spark'

    def writeDetail(self, flag, spark_data):
        """
          write recommend orders of each driver.
        Input
        ------
          detail       :pandas.DataFrame;

        Output
        ----

        # translate pandas.DataFrame into spark DataFrame;
        > dfSpark = spark.createDataFrame(pandas_df)
        """
        # spark = SparkSession.builder \
        #     .master("local[2]") \
        #     .appName("Spark-on-Hbase") \
        #     .enableHiveSupport() \
        #     .getOrCreate()
        #
        # recSys = recommendSys()
        # flag,spark_data = recSys.multiProcess(spark)
        if flag == 1:

            # 设置打印warn及以上级别的日志
            # spark.sparkContext.setLogLevel("WARN")

            # df = spark.read.csv("file:///home/admin/mqf/tools/outputDriver.csv", header=True)

            df = spark_data
            # df.write.format(self.data_source_format) \
            #     .option('hbase.table', 'lyt_rp:driver_matched_order') \
            #     .option('hbase.config.resources', 'file:///etc/hbase/conf/hbase-site.xml') \
            #     .option('hbase.columns.mapping', '_c0 STRING :key, \
            #     _c1 STRING orderInfo:tpPublishedOrderId, \
            #     _c2 STRING orderInfo:orderNo, \
            #     _c3 STRING orderInfo:shipperPartyId, \
            #     _c4 STRING orderInfo:shipperCompanyName, \
            #     _c5 STRING orderInfo:totalFee, \
            #     _c6 STRING orderInfo:goodsType, \
            #     _c7 STRING orderInfo:weight, \
            #     _c8 STRING orderInfo:volume, \
            #     _c9 STRING orderInfo:vehicleTypes, \
            #     _c10 STRING orderInfo:vehicleLengths, \
            #     _c11 STRING orderInfo:expireDate, \
            #     _c12 STRING orderInfo:loadingDate, \
            #     _c13 STRING orderInfo:offDate, \
            #     _c14 STRING orderInfo:linkMobile, \
            #     _c15 STRING orderInfo:linkMan, \
            #     _c16 STRING orderInfo:startProvince, \
            #     _c17 STRING orderInfo:startCity, \
            #     _c18 STRING orderInfo:startTown, \
            #     _c19 STRING orderInfo:endProvince, \
            #     _c20 STRING orderInfo:endCity, \
            #     _c21 STRING orderInfo:endTown, \
            #     _c22 STRING orderInfo:publishType, \
            #     _c23 STRING orderInfo:flag, \
            #     _c24 STRING orderInfo:inputDate, \
            #     _c25 STRING orderInfo:industry, \
            #     _c26 STRING orderInfo:recommendScore') \
            #     .option('hbase.use.hbase.context', False) \
            #     .option('hbase-push.down.column.filter', False) \
            #     .save()
            df.write.format(self.data_source_format) \
                .option('hbase.table', 'lyt_rp:driver_matched_order') \
                .option('hbase.config.resources', 'file:///etc/hbase/conf/hbase-site.xml') \
                .option('hbase.columns.mapping', 'driverPartyId STRING :key, \
                tpPublishedOrderId STRING orderInfo:tpPublishedOrderId, \
                orderNo STRING orderInfo:orderNo, \
                shipperPartyId STRING orderInfo:shipperPartyId, \
                shipperCompanyName STRING orderInfo:shipperCompanyName, \
                totalFee STRING orderInfo:totalFee, \
                goodsType STRING orderInfo:goodsType, \
                goodsName STRING orderInfo:goodsName, \
                weight STRING orderInfo:weight, \
                volume STRING orderInfo:volume, \
                vehicleTypes STRING orderInfo:vehicleTypes, \
                vehicleLengths STRING orderInfo:vehicleLengths, \
                expireDate STRING orderInfo:expireDate, \
                loadingDate STRING orderInfo:loadingDate, \
                offDate STRING orderInfo:offDate, \
                linkMobile STRING orderInfo:linkMobile, \
                linkMan STRING orderInfo:linkMan, \
                startProvince STRING orderInfo:startProvince, \
                startCity STRING orderInfo:startCity, \
                startTown STRING orderInfo:startTown, \
                endProvince STRING orderInfo:endProvince, \
                endCity STRING orderInfo:endCity, \
                endTown STRING orderInfo:endTown, \
                publishType STRING orderInfo:publishType, \
                flag STRING orderInfo:flag, \
                inputDate STRING orderInfo:inputDate, \
                industry STRING orderInfo:industry, \
                version STRING orderInfo:version, \
                industrySimilarity STRING orderInfo:industrySimilarity, \
                sourceRoute STRING orderInfo:sourceRoute, \
                historyRoute STRING orderInfo:historyRoute, \
                routeNumber STRING orderInfo:routeNumber, \
                totalRouteNumber STRING orderInfo:totalRouteNumber, \
                routeRatio STRING orderInfo:routeRatio, \
                routePair STRING orderInfo:routePair, \
                routeSimilarity STRING orderInfo:routeSimilarity, \
                industryRatio STRING orderInfo:industryRatio, \
                recommendScore STRING orderInfo:recommendScore') \
                .option('hbase.use.hbase.context', False) \
                .option('hbase-push.down.column.filter', False) \
                .save()
        else:
            pass
        # spark.stop()

    def writeOwners(self, flag, spark_data):
        """
          write recommend orders of each driver.
        Input
        ------
          detail       :pandas.DataFrame;

        Output
        ----

        # translate pandas.DataFrame into spark DataFrame;
        > dfSpark = spark.createDataFrame(pandas_df)
        """
        # spark = SparkSession.builder \
        #     .master("yarn") \
        #     .appName("Spark-on-Hbase") \
        #     .enableHiveSupport() \
        #     .config("spark.sql.warehouse.dir", self.warehouse_location) \
        #     .getOrCreate()
        # spark = SparkSession.builder \
        #     .master("yarn") \
        #     .appName("Spark-on-Hbase") \
        #     .enableHiveSupport() \
        #     .getOrCreate()
        # conf = SparkConf()
        # config = (())
        # spark = SparkSession.builder.getOrCreate()
        # spark = SparkSession.builder \
        #     .master("local[2]") \
        #     .appName("Spark-on-Hbase") \
        #     .enableHiveSupport() \
        #     .getOrCreate()
        # 设置打印warn及以上级别的日志
        # spark.sparkContext.setLogLevel("WARN")
        #
        # recSys = recommendSys()
        # flag,spark_data = recSys.multiProcess(spark)
        #
        # flag,spark_data = recommendSys.multiProcess(spark)
        if flag == 1:
            df = spark_data.select("driverPartyId", "shipperCompanyName")
            # print(df.printSchema())
            # df = df.withColumn("driverPartyId", concat(df["driverPartyId"], lit("_list")))

            df.write.format(self.data_source_format) \
                .option('hbase.table', 'lyt_rp:driver_matched_owner') \
                .option('hbase.config.resources', 'file:///etc/hbase/conf/hbase-site.xml') \
                .option('hbase.columns.mapping', 'driverPartyId STRING :key, \
                shipperCompanyName STRING ownerList:shipperCompanyName') \
                .option('hbase.use.hbase.context', False) \
                .option('hbase-push.down.column.filter', False) \
                .save()
        else:
            pass
        # spark.stop()


    def main(self):
        """
        :return:
        """
        start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        spark = SparkSession.builder \
            .appName("Spark-on-Hbase") \
            .enableHiveSupport() \
            .getOrCreate()
        recSys = recommendSys()
        flag,spark_data = recSys.multiProcess(spark)
        log.logger.info(spark_data.count())

        engine = create_engine(
            'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format("bd_aiData",
                                                                 "4U754cJ65CaLPianhci3",
                                                                 "10.33.64.9",
                                                                 3306,
                                                                 "bd_aiData"))



        #spark_data = spark_data.withColumn("driverPartyId",concat(spark_data['driverPartyId'].cast("String"),lit("_")))

        self.writeDetail(flag,spark_data)
        self.writeOwners(flag,spark_data)

        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        table = 'spark_write_hbase'
        df = pd.DataFrame({
            # 'Ranks': id,
            'start_time': start_time,
            'end_time': end_time,
            'num_len': spark_data.count()
        }, index=[0])
        if df.shape[0] > 0:
            # delete_data(table)
            pd.io.sql.to_sql(df,
                             table,
                             engine,
                             schema="bd_aiData",
                             if_exists="append",
                             index=False)
            logging.info("Successfully write features into mysql")
        else:
            logging.warning("Empty data")

        print(spark_data.printSchema())
        spark.stop()


if __name__ == "__main__":

    # while True:
    #     time.sleep(1)
    #     cic = checKIfChange()
    #     if cic == 0:
            start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            log.logger.info(start_time)
            spkWriHb = sparkWriteHbase()
            spkWriHb.main()
            end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            log.logger.info(end_time)
            log.logger.info("Successfully write data into Hbase.")





"""
spark-submit --master local[2] \
--queue root.CBT \
--num-executors 6 \
--driver-memory 5g \
--executor-memory 6g \
--executor-cores 4 \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.executor.extraJavaOptions="-XX:MaxPermSize=4068m" \
--conf spark.kryoserializer.buffer.max=2000m \
--conf spark.driver.maxResultSize=6g \
--conf spark.speculation=false \
--conf spark.debug.maxToStringFields=200 \
/home/admin/zr/lytrecommendsystem_spark/model/sparkWriteHbase.py >/dev/null 2>&1 &



nohup spark-submit --master local[3] \
--queue root.RT \
--num-executors 6 \
--driver-memory 6g \
--executor-memory 8g \
--executor-cores 6 \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.executor.extraJavaOptions="-XX:MaxPermSize=4068m" \
--conf spark.kryoserializer.buffer.max=2000m \
--conf spark.driver.maxResultSize=6g \
--conf spark.speculation=false \
--conf spark.debug.maxToStringFields=200 \
/home/admin/zr/lytrecommendsystem_spark/model/sparkWriteHbase.py >/dev/null 2>&1 &


base环境执行
10.77.0.247启动pyspark
1. pyspark --master local
2.df=spark.read.csv("file:///home/admin/mqf/tools/outputDriver.csv", header=True)
3.from pyspark.sql.functions import udf, col,lit,concat
4.df2 = df2.withColumn("driverPartyID", concat(df2["driverPartyID"], lit("_list")))
"""

command2 = "spark-submit --master local[8] --queue root.CBT --num-executors 6 --driver-memory 6g --executor-memory 8g --executor-cores 6 --conf spark.shuffle.service.enabled=false --conf spark.dynamicAllocation.enabled=false --conf spark.executor.extraJavaOptions=\"-XX:MaxPermSize=4068m\" --conf spark.kryoserializer.buffer.max=2000m --conf spark.driver.maxResultSize=6g --conf spark.speculation=false --conf spark.debug.maxToStringFields=200 /home/admin/zr/lytrecommendsystem_spark/model/sparkWriteHbase.py"
