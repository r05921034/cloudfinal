PYSPARK_PYTHON=python3 ../bin/spark-submit --packages com.stratio.datasource:spark-mongodb_2.11:0.12.0 try3.py

PYSPARK_PYTHON=python3 ../spark-2.1.0-bin-hadoop2.7/bin/spark-submit --packages com.stratio.datasource:spark-mongodb_2.11:0.12.0 try3.py

please use this cmd to submit the sparkprogram 

spark default is using python2 and --packages is to set the spark io

