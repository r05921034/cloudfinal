# encoding=utf-8
from pyspark.sql import SparkSession
from pyspark import SparkConf,SparkContext
from pymongo import MongoClient
conf=SparkConf().setMaster("local").setAppName("myapp")
sc=SparkContext(conf=conf)
output=list()
output2=list()
#class process(ob) 
def calpush(s,source,output):
	findtitle=source.filter(lambda x: x.Title.find(s.Title)>=0)
	if findtitle.isEmpty():
		findtitle2=sc.parallelize([0])
	else:
		findtitle2=findtitle.map(score)
	findtitle3=findtitle2.map(lambda x: x if x>=0 else 0)
	findtitle4=findtitle3.reduce(lambda x,y:x+y)
	total=findtitle2.reduce(lambda x,y:abs(x)+abs(y))
	return findtitle4,total
'''		
def forweigh(x):
	if(x>=0):
		x=x+y
		return x=x+y
	else:
		return x=x+0
'''		
def score(x):
	if x.Category.find('好')>=0:
		if x.Push.find('爆')>=0:
			return	100
		elif x.Push.find('XX')>=0:
			return -100
		else:
			return int(x.Push.replace('X','-'))*1+1
	elif x.Category.find('負')>=0:
		if x.Push.find('XX')>=0:
			return -100
		elif x.Push.find('爆')>=0:
			return 100
		else:
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
#out2=my_spark.sql("SELECT Title FROM temp2")
out2=my_spark.sql("SELECT * FROM temp2")
out2=out2.rdd.cache()
title=out2.collect()
for s in title:
	sco,total=calpush(s,out,output)
	formovietitle=out.filter(lambda x: x.Title.find(s.Title)>=0)
	if total!=0:
		sco=(abs(sco)/total)*5
	output2.append({"Movietitle":s.Title,"Score":float(sco),"Img":s.Img,"Rate":s.Rate,"Grade":s.Grade,"Time":s.Time,"Date":s.Date})
	if formovietitle.isEmpty():
		output.append({"Movietitle":s.Title,
			"Title":'NAN',
                        "Push":'NAN',
                        "Data":'NAN',
                        "Link":'NAN',
                        "Category":'NAN',
                        "Pre_rid":'NAN'
                        })

	else:
		for x in formovietitle.collect():
			 output.append({
	                "Movietitle":s.Title,
			"Title":x.Title,
			"Push":x.Push,
			"Data":x.Date,
			"Link":x.Link,
			"Category":x.Category,
			"Pre_rid":x._id
			})
	#output2.append({"movietitle":s.Title,"score":sco,"Img":s.Img,"Rate":s.Rate,"Grade":s.Grade})


wr=my_spark.createDataFrame(output)
wr.write.format("com.stratio.datasource.mongodb").options(host="localhost:27017", database="mydb", collection="fortest1").mode("append").save()
wr.show()

wr2=my_spark.createDataFrame(output2)
wr2.write.format("com.stratio.datasource.mongodb").options(host="localhost:27017", database="mydb", collection="fortest2").mode("append").save()
wr2.show()



'''
client=MongoClient('mongodb://localhost:27017/')
db=client.mydb
movie
'''	

