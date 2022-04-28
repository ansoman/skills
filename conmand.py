#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    @Time      : 2022/4/28 13:30
    @Author    : fab
    @File      : conmand.py
    @Email     : xxxx@etransfar.com
"""
import os

os.chdir('/home/admin/mqf/tools/')

command1 = "spark-submit --master local[3] \
--queue root.RT \
--num-executors 10 \
--driver-memory 10g \
--executor-memory 15g \
--executor-cores 10 \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.executor.extraJavaOptions=\"-XX:MaxPermSize=4068m\" \
--conf spark.kryoserializer.buffer.max=2000m \
--conf spark.driver.maxResultSize=6g \
--conf spark.speculation=false \
--conf spark.debug.maxToStringFields=200 \
--conf spark.pyspark.python=/home/admin/anaconda3/envs/python-3.6-cxd/bin/python \
--conf spark.pyspark.driver.python=/home/admin/anaconda3/envs/python-3.6-cxd/bin/python \
/home/admin/mqf/tools/writeHive.py"

os.system(command1)

"""
通过python写数据到hive
writeHive.py通过该脚本处理完数据以及编写写入数据的功能
执行该脚本即可把数据写入或者执行command.sh脚本
"""
