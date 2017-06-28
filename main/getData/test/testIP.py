#encoding=utf8
import urllib
from bs4 import BeautifulSoup
from urllib.request import urlopen
from urllib.request import Request
User_Agent = 'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0'
header = {}
header['User-Agent'] = User_Agent

url = 'http://www.kxdaili.com/ipList/1.html#ip'
req = Request(url,headers=header)
res = urlopen(req).read()

soup = BeautifulSoup(res)
ips = soup.findAll('tr')
f = open("../src/proxy.txt","w")

for x in range(1,len(ips)):
    ip = ips[x]
    tds = ip.findAll("td")
    ip_temp = tds[0].contents[0]+"\t"+tds[4].contents[0]+"\n"
    # print tds[2].contents[0]+"\t"+tds[3].contents[0]
    f.write(ip_temp)