# -*- coding: utf-8 -
import requests
import urllib.parse
import time 
from bs4 import BeautifulSoup
from multiprocessing import Pool

INDEX = 'https://www.ptt.cc/bbs/movie/index.html'


class pttcrawler:
    def __init__(self,pages = 0, INDEX = ""):
        self.INDEX = INDEX
        self.pages = pages
        self.NOT_EXIST = BeautifulSoup('<a>本文已被刪除</a>', 'lxml').a
        self.posts = []
    def get_posts_list(self, url):
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'lxml')

        posts = list()
        for article in soup.find_all('div', 'r-ent'):
            meta = article.find('div', 'title').find('a') or self.NOT_EXIST
            push = article.find('div', 'nrec').getText()
            if not push : 
                push = '0'
            title = str(meta.getText().strip())
            if title.startswith("Re: "):
                title = title[4:].split("]")[1]
                catagory = "Reply"
            else :
                title = title.split("]")
                if len(title) > 1:
                    catagory = title[0].replace(' ','').replace('[','')
                    title = title[1]
                else :
                    catagory = "Remove" 
                    title = title[0]
            posts.append({
                'Title':title,
                'Link': meta.get('href'),
                "Category" : catagory,
                'Push': push,
                'Date': article.find('div', 'date').getText(),
                'Author': article.find('div', 'author').getText(),
            })
            

        next_link = soup.find('div', 'btn-group-paging').find_all('a', 'btn')[1].get('href')

        return posts, next_link

    def get_paged_meta(self, page):
        page_url = self.INDEX
        all_posts = list()
        for i in range(page):
            posts, link = self.get_posts_list(page_url)
            all_posts += posts
            page_url = urllib.parse.urljoin(INDEX, link)
        return all_posts

    def get_pages(self, num):
        page_url = self.INDEX
        all_posts = list()
        for i in range(num):
            posts, link = self.get_posts_on_page(page_url)
            all_posts += posts
            page_url = urllib.parse.urljoin(self.INDEX, link)
        return all_posts

    def get_articles(self, metadata):
        post_links = [meta['Link'] for meta in metadata]
        with Pool(processes=8) as pool:
            contents = pool.map(self.fetch_article_content, post_links)
            return contents

    def fetch_article_content(self, link):
        url = urllib.parse.urljoin(self.INDEX, link)
        response = requests.get(url)
        return len(response.text)

    def run(self) :
       
        metadata = self.get_paged_meta(self.pages)
        articles = self.get_articles(metadata)

        for post, content in zip(metadata, articles):
            post["Content"] = content;
            self.posts.append(post)


if __name__ == '__main__':
    mycrawler = pttcrawler(20,INDEX)
    start = time.time()
    mycrawler.run()
    print(mycrawler.posts)
    # print('花費: %f 秒' % (time.time() - start))
    # print('共%d項結果：' % len(mycrawler.posts))
    # for p in mycrawler.posts:
    #     print('{0} {1} {2} {3: <15} {4}, 網頁內容共 {5} 字'.format(
    #             p['Push'], p['Category'],p['Date'], p['Author'], p['Title'], len(p['Content'])))