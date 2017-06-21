# -*- coding: utf-8 -
from pymongo import MongoClient
import crawler
INDEX = 'https://www.ptt.cc/bbs/movie/index.html'
if __name__ == "__main__":
	client = MongoClient('mongodb://cc2017:' + "12345" + '@127.0.0.1/tracking?authSource=admin')
	#client = MongoClient(hostname=['localhost@27017']
	#client = MongoClient('mongodb://localhost:27017/')
	db = client.mydb
	movie_collection = db.movie

	mycrawler = crawler.pttcrawler(1000,INDEX)
	#start = time.time()
	mycrawler.run()
	#print('花費: %f 秒'%(time.time() - start))
	#print('共%d項結果：'%len(mycrawler.posts))
	movie_collection.insert_many(mycrawler.posts)
	for post in movie_collection.find():
		print(post)
