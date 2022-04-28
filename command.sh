spark-submit --master local[3] \
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
/home/admin/mqf/tools/writeHive.py