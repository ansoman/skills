#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    @Time      : 2022/4/28 10:44
    @Author    : fab
    @File      : writeHive.py
    @Email     : xxxx@etransfar.com
"""
import pandas as pd
from pyspark.sql import SparkSession


def main():
    data = {"drivervehicleplate": ["浙A12345", "浙A12346", "浙A12347"],
            "rn": [5, 6, 7]}
    df = pd.DataFrame(data)
    print(df)

    print("build spark engine.")
    spark = SparkSession.builder.appName("dataFrame_write_hive").getOrCreate()
    print("pandas data frame to spark frame.")
    spark_df = spark.createDataFrame(df)
    print("start writing.")
    spark_df.write.mode('overwrite').format("hive").saveAsTable("data_analysis.write_hive_test")
    spark.close()
    print("close")


if __name__ == "__main__":
    main()

