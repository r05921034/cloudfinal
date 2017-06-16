# coding=utf-8
from pyspark import SparkConf,SparkContext
from pyspark import StorageLevel

def getmatch(x):       
	global count
	global count2
	count+=1
	count2=count2+1
	return x.find("阿拉伯")
def forcount(x):
	if x ==-1:
		return 0
	else:
		return 1

conf=SparkConf().setMaster("local").setAppName("My App")
sc=SparkContext(conf=conf)
count2=0
count=sc.accumulator(0)

#lines=sc.textFile("full.txt",use_unicode=False).map(lambda x:x.decode('utf8')).flatMap(lambda x:x.split(""))


lines2=sc.textFile("chinese.txt",use_unicode=False).flatMap(lambda x:x.split(" "))

line3=lines2.map(lambda x:x.replace(".","")).persist(StorageLevel.MEMORY_ONLY)
line5=line3.map(lambda x:x.replace(",",""))
line4=line3.map(getmatch)
line=line5.collect()
#print(count,'aaaaaaa')
#print(count2,'bbbbbb')
#line4=line3.map(lambda x:x.find("阿拉伯"))
#line6=line4.collect()
#line6=line4.map(forcount)
#line=line6.reduce(lambda x,y:x+y)
#print(count,count2,'wwwww')
#print(count2,'sssss')
print(line)

for x in line:
	print(x)#decode('utf-8'))


