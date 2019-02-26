import crawl
import data_process
from queue import Queue
import threading
import report
import datetime
import config
import json
import time
import os


def load(confg,year_month):
    with open(confg)as f:
        j = json.loads(f.read())
        config.URL = j['url']
        config.BASE_URL = config.URL[:len(config.URL) - 12]
        config.HOUSE_TABLE = j['house_table'] + year_month
        config.COMMUNITY_TABLE = j['community_table'] + year_month
        config.MYSQL_NAME = j['mysql_name']
        config.MYSQL_PASSWORD = j['mysql_password']
        config.DATABASE_NAME = j['database_name']
        config.USE_REPORT = j['use_email']
        config.EMAIL_NAME = j['email_name']
        config.EMAIL_PW = j['email_pw']
        config.EMAIL_RECEIVER = j['email_receiver']


def get_house_url(data_p):
    if data_p.house_url_count() == 0 and data_p.img_url_count() == 0:
        print('数据库无可爬连接，开始收集连接')
        href_list = crawl.get_small_region_list()
        print('已找到{}个小地点'.format(str(len(href_list))))
        lock_set = threading.Lock()
        href_queue = Queue()
        for href in href_list:
            href_queue.put(href)
        url_set = set()
        thread_list = list()
        thread_count = 15
        for i in range(thread_count):
            thread_list.append(crawl.CrawlHouseUrlThread(href_queue, url_set, lock_set))
        for thread in thread_list:
            thread.start()
        for thread in thread_list:
            thread.join()
        print('已找到{}个房子链接'.format(str(len(url_set))))
        data_p.insert_house_url_set(url_set)


def main():
    '''
    爬虫程序分为几个部分
    crawl.py: 爬虫部分，包括线程
    其中的crawl函数为解析网页函数，返回爬取信息的list
    report.py: 发邮件报告程序进度
    data_process.py: 封装数据库的一些操作
    sqlite3_connector_pool: sqlite3的连接池

    运行程序只需要运行  run.py
    '''
    data_p = data_process.DataProcess()
    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 现在
    print(nowTime)
    data_p.create_table()
    get_house_url(data_p)

    is_end = [False]

    report_thread = report.RepoterThread(data_p,is_end)
    report_thread.setDaemon(True)
    report_thread.start()

    print('数据库已有连接，开始爬取')
    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 现在
    print(nowTime)
    out_queue = Queue()
    out_thread = crawl.OutThread(out_queue)
    out_thread.start()

    thread_count = 15
    thread_list = list()
    for i in range(thread_count):
        thread_list.append(crawl.CrawlHouseThread(out_queue,data_p))
    for thread in thread_list:
        thread.start()
    for thread in thread_list:
        thread.join()

    print('开始爬取图片')
    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 现在
    print(nowTime)
    thread_count = 15
    thread_list = list()
    for i in range(thread_count):
        thread_list.append(crawl.DownloadImgThread(data_p))
    for thread in thread_list:
        thread.start()
    for thread in thread_list:
        thread.join()

    print('爬取结束')
    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 现在
    print(nowTime)

    is_end[0] = True
    data_p.release()


def run():
    while True:
        time_object = time.localtime(time.time())
        if time_object.tm_mday < 10:
            time.sleep(3600)
            continue
        year_month = str(time_object.tm_year) + '_' + str(time_object.tm_mon)
        file_name = 'data/'+year_month+'.json'
        if not os.path.exists(file_name):
            with open('config_list.json')as f:
                config_list = json.loads(f.read())
            with open(file_name,'w')as f:
                f.write(json.dumps(config_list))
        while True:
            with open(file_name)as f:
                config_list = json.loads(f.read())
            if len(config_list) == 0:
                time.sleep(3600)
                break
            config = config_list[0]
            print('开始爬取' + config)
            load(config,year_month)
            # main()
            time.sleep(5)
            print('结束爬取' + config)
            config_list.pop(0)
            with open(file_name,'w')as f:
                f.write(json.dumps(config_list))



if __name__ == '__main__':
    run()
