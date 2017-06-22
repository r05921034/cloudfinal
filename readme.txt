PYSPARK_PYTHON=python3 ../bin/spark-submit --packages com.stratio.datasource:spark-mongodb_2.11:0.12.0 spark.py

PYSPARK_PYTHON=python3 ../spark-2.1.0-bin-hadoop2.7/bin/spark-submit --packages com.stratio.datasource:spark-mongodb_2.11:0.12.0 spark.py

   please use this second cmd to submit the sparkprogram, spark default is using python2, so set python3 and --packages is to set the spark i/o

python db.py

db.py will import crawler.py to fetch ptt movie's list, and export to mongodb

python3 db2.py

db2.py will import crawler3.py to fetch  影劇圈圈 list, and export to mongdb



