# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import requests
import json
import crawler
import time
INDEX = 'https://www.ptt.cc/bbs/NTU/index.html'
class DataPipe:
    def __init__(self, data):

        self.data = data
        self.spark = SparkSession.builder.appName("PTT_crawler").enableHiveSupport().getOrCreate()
    def run(self):
        df = self.spark.createDataFrame(self.data)
        df.printSchema()
        df.createOrReplaceTempView("PPTcrawler")
        self.spark.sql("DROP TABLE IF EXISTS PPTcrawler")
        df.write.saveAsTable("PPTcrawler")
        sqlDF = self.spark.sql("SELECT * FROM PPTcrawler WHERE Catagory == '徵才'")
        sqlDF.select("Catagory", "Author", "Date", "Title").show()

if __name__ == "__main__":
    mycrawler = crawler.pttcrawler(5,INDEX)
    start = time.time()
    mycrawler.run()
    # print('花費: %f 秒' % (time.time() - start))
    # print('共%d項結果：' % len(mycrawler.posts))
    # for p in mycrawler.posts:
    #     print('{0} {1} {2} {3: <15} {4}, 網頁內容共 {5} 字'.format(
    #             p['Push'], p['Catagory'],p['Date'], p['Author'], p['Title'], len(p['Content'])))
    mypipe = DataPipe(mycrawler.posts)
    mypipe.run()