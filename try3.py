# encoding=utf-8
from pyspark.sql import SparkSession
from pyspark import SparkConf,SparkContext
from pymongo import MongoClient
conf=SparkConf().setMaster("local").setAppName("myapp")
sc=SparkContext(conf=conf)
output=list()
#class process(ob) 
def calpush(s,source,output):
	findtitle=source.filter(lambda x: x.Title.find(s.Title)>=0)
	if findtitle.isEmpty():
		findtitle2=sc.parallelize([0])
	else:
		findtitle2=findtitle.map(score)
	findtitle3=findtitle2.reduce(lambda x,y:x+y)
	return findtitle3
		
	
def score(x):
	if x.Category.find('好')>=0:
		return int(x.Push.replace('X','-'))*1+1
	elif x.Category.find('負')>=0:
		return int(x.Push.replace('X','-'))*-1-1
	else :
		return 0
def forappend(x,sco,s):
	output.append({
		"movietitle":s.Title,
                "score":sco,
		"Title":x.Title,
		"Push":x.Push,
		"Data":x.Date,
		"Link":x.Link

	
	})

my_spark = SparkSession.builder.appName("myApp").getOrCreate()
df = my_spark.read.format("com.stratio.datasource.mongodb").options(host="localhost:27017", database="mydb", collection="movie").load()
df2=my_spark.read.format("com.stratio.datasource.mongodb").options(host="localhost:27017", database="mydb", collection="movietitle").load()
df.printSchema()
df2.printSchema()
#df.show()
#df2.show()
df.createOrReplaceTempView("temp")
df2.createOrReplaceTempView("temp2")
out=my_spark.sql("SELECT * FROM temp")
out=out.rdd.cache()
out2=my_spark.sql("SELECT Title FROM temp2")
out2=out2.rdd.cache()
title=out2.collect()
for s in title:
	sco=calpush(s,out,output)
	formovietitle=out.filter(lambda x: x.Title.find(s.Title)>=0)
	if formovietitle.isEmpty():
		output.append({"movietitle":s.Title,
                        "score":sco
        })
	else:
		for x in formovietitle.collect():
			 output.append({
	                "movietitle":s.Title,
        	        "score":sco,
			"Title":x.Title,
			"Push":x.Push,
			"Data":x.Date,
			"Link":x.Link,
			"Category":x.Category,
			"pre_rid":x._id
			})
wr=my_spark.createDataFrame(output)
wr.write.format("com.stratio.datasource.mongodb").options(host="localhost:27017", database="mydb", collection="sparkoutput").mode("append").save()
wr.show()
'''
client=MongoClient('mongodb://localhost:27017/')
db=client.mydb
movie
'''	
print(output)
