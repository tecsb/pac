#coding=utf-8
#author : zx
#date   : 2015/07/27

import requests
import sqlite3
import time
import string
import random
from lxml import etree
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
#ua头信息 get时可以随机使用


def search(conn,cursor):
    headers = [
        {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko'},
        {'User-Agent','Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11'},
        { "User-Agent":"Mozilla/5.0 (Linux; U; Android 4.1; en-us; GT-N7100 Build/JRO03C) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30"},
        { "User-Agent":"Mozilla/5.0 (compatible; MSIE 10.0; Windows Phone 8.0; Trident/6.0; IEMobile/10.0; ARM; Touch; NOKIA; Lumia 520)"},
        { "User-Agent":"Mozilla/5.0 (BB10; Touch) AppleWebKit/537.10+ (KHTML, like Gecko) Version/10.0.9.2372 Mobile Safari/537.10+"},
        { "User-Agent":"Mozilla/5.0 (Linux; Android 4.4.2; GT-I9505 Build/JDQ39) AppleWebKit/537.36 (KHTML, like Gecko) Version/1.5 Chrome/28.0.1500.94 Mobile Safari/537.36"}
    ]
    #城市入口页面
    #我只抓的青岛本地
    #其它城市或全国城市可通过这个页面抓取城市列表http://m.anjuke.com/cityList
    url = 'http://m.anjuke.com/bj/xiaoqu/'
    req = requests.get(url)

    cookie = req.cookies.get_dict()
    print req.text.encode('utf-8')
    #链接<a href="/database/database.html" target="_blank">数据库</a>
    # conn = MySQLdb.connect('localhost', '*****', '******', '***', charset='utf8')
    # cursor = conn.cursor()
    sql = "insert into house values(?,?,?,?,?,?,?,?)"
    sql_v = []
    page = etree.HTML(req.text)
    districtHTML = page.xpath(u"//div[@class='listcont cont_hei']")[0]
    #采集目标城市的各行政区域url
    #当然如果不想区分行政区可以直接抓“全部” 即上面url中的所有小区及<a href="/tags.php/%B7%D6%D2%B3/" target="_blank">分页</a>
    districtUrl = {}
    i = 0
    for a in districtHTML:
        if i==0:
            i = 1
            continue
        districtUrl[a.text] = a.get('href')
    #开始采集
    choosed ={}
    total_all = 0
    for i,j in districtUrl.items():
       print i
       print j.rstrip('/')
       if i== u'海淀':
           choosed[i] = j
    '''
    for k,u in districtUrl.items():
    '''
    print choosed
    for k,u in choosed.items():

        p = 1 #分页
        while True:
            header_i = random.randint(0, len(headers)-1)
            url_p = u.rstrip('/') + '-p' + str(p)
            r = requests.get(url_p, cookies=cookie, headers=headers[header_i])
            page = etree.HTML(r.text) #这里转换大小写要按情况...
            communitysUrlDiv = page.xpath(u"//div[@class='items']")[0]
            total = len(communitysUrlDiv)
            i = 0
            for a in communitysUrlDiv:
                i+=1
                try:
                    r = requests.get(a.get('href'), cookies=cookie, headers=headers[header_i])
                except:
                    time.sleep(1)
                    continue
                #抓取时发现有少量404页会直接导致程序报错退出- -!
                #唉 说明代码写的还不够健壮啊
                #加了if判断和try， 错误时可以跳过或做一些简单处理和调试...
                if r.status_code == 404:
                    continue
                page = etree.HTML(r.text)
                try:
                    name = page.xpath(u"//h1[@class='f1']")[0].text
                    print name
                except:
                    print a.get('href')
                #有少量小区未设置经纬度信息
                #只能得到它的地址了
                try:
                    latlng = page.xpath(u"//a[@class='comm_map']")[0]
                    lat = latlng.get('lat')
                    lng = latlng.get('lng')
                    # address = latlng.get('address')
                except:
                    lat = ''
                    lng = ''
                    # address = page.xpath(u"//span[@class='rightArea']/em")[0].text
                try:
                    price = page.xpath(u"//i[@class='f4']")[0].text

                    print price
                except:
                    pass
                try:
                    inc = page.xpath(u"//em[@class='fe54c00']")[0].text
                    if inc is None:
                        inc = page.xpath(u"//em[@class='fe54c00']/arrow")[0].text
                    print inc
                except:
                    pass
                try:
                    address = page.xpath(u"//span[@class='rightArea']/em")[0].text
                    print address
                except:
                    address=u'未知'

                sql_v.append((a.get('href'),name, price, inc,address,lat,lng, k))
                print "\r\r\r",
                print u"正在下载 %s 的数据,第 %d 页,共 %d 条，当前:".encode('gbk') %(k.encode('gbk'),p, total) + string.rjust(str(i),3).encode('gbk'),
                time.sleep(0.5) #每次抓取停顿
            #执行插入数据库
            for i in sql_v:
                cursor.execute(sql, i)
            conn.commit()
            sql_v = []
            time.sleep(3)  #每页完成后停顿
            total_all += total
            print ''
            print u"成功入库 %d 条数据，总数 %d".encode('gbk') % (total, total_all)
            if total < 500:
                break
            else:
                p += 1
    #及时关闭数据库 做个好孩子 任务完成~
    # cursor.close()
    # conn.close()
    print u'所有数据采集完成! 共 %d 条数据'.encode('gbk') % (total_all)
    raw_input()
def start_db():
    conn  = sqlite3.connect('test2.db')
    cursor = conn.cursor()
    sql="CREATE TABLE IF NOT EXISTS house (commuid varchar(50),name varchar(20),price varchar(20),inc varchar(20),address varchar(100),lat varchar(20),lng varchar(20),id integer)";
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()
if __name__ == '__main__':
    # start_db()

    conn  = sqlite3.connect('test2.db')
    cursor = conn.cursor()
    # search(conn,cursor)
    # cursor.execute('select * from house where id ="海淀" ')
    # conn.commit()
    import pandas as pa
    df = pa.read_sql_query('select * from house',conn)
    df.to_excel('all.xls')
    # print len(df.commuid.tolist())
    # df2 = df.drop_duplicates(subset='commuid')
    # print len(df2.commuid.tolist())
    # cursor.execute('delete from house where id ="海淀" ')
    # conn.commit()
    # df_hd =  pa.read_excel('haidian.xls')
    # df_hd = df_hd.set_index('commuid')
    # df_hd.to_sql('house',conn,if_exists ='append')
    cursor.close()
    conn.close()