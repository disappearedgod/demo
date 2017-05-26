from scrapy.http import Request
import scrapy
from bs4 import BeautifulSoup
import re
import requests
"""
    spider脚本
"""
class Myspider(scrapy.Spider):
    name = 'anjuke_gz'
    allowed_domains = ['http://gz.fang.anjuke.com/loupan/']
    start_urls = ["http://gz.fang.anjuke.com/loupan/all/p{}/".format(i) for i in range(39)]

    def parse(self, response):
        soup = BeautifulSoup(response.text,"lxml")
        content=soup.find_all(class_="items-name") #返回每个楼盘的对应数据
        for item in content:
            code=item["href"].split("/")[-1][:6]
            real_href="http://gz.fang.anjuke.com/loupan/canshu-{}.html?from=loupan_tab".format(code) #拼凑出楼盘详情页的url
            res=requests.get(real_href)
            soup = BeautifulSoup(res.text,"lxml")
            a = re.findall(r'<div class="name">(.*?)</div>', str(soup))
            b = soup.find_all(class_="des")
            data = {}
            for (i, j) in zip(range(len(b)), a):
                data[j] = b[i].text.strip().strip("\t")
                data["url"] = real_href
            yield data