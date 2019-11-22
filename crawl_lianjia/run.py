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


def print_msg(msg):
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(now + ': ' + msg)


def load(config_name,year_month):
    with open('config/' + config_name)as f:
        j = json.loads(f.read())
        config.NAME = config_name[:len(config_name)-5]
        config.URL = j['url']
        config.BASE_URL = config.URL[:len(config.URL) - 12]
        config.HOUSE_TABLE = j['house_table'] + '_' + year_month
        config.COMMUNITY_TABLE = j['community_table'] + '_' + year_month
        config.MYSQL_NAME = j['mysql_name']
        config.MYSQL_PASSWORD = j['mysql_password']
        config.DATABASE_NAME = j['database_name']
        config.USE_REPORT = j['use_email']
        config.EMAIL_NAME = j['email_name']
        config.EMAIL_PW = j['email_pw']
        config.EMAIL_RECEIVER = j['email_receiver']


def get_house_url(data_p):
    if data_p.house_url_count() == 0 and data_p.img_url_count() == 0:
        print_msg('数据库无可爬连接，开始收集连接')
        href_list = crawl.get_small_region_list()
        print_msg('已找到{}个小地点'.format(str(len(href_list))))
        lock_set = threading.Lock()
        href_queue = Queue()
        for href in href_list:
            href_queue.put(href)
        url_set = set()
        thread_list = list()
        thread_count = 5
        for i in range(thread_count):
            thread_list.append(crawl.CrawlHouseUrlThread(href_queue, url_set, lock_set))
        for thread in thread_list:
            thread.start()
        for thread in thread_list:
            thread.join()
        print_msg('已找到{}个房子链接'.format(str(len(url_set))))
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
    data_p.create_table()
    get_house_url(data_p)

    # 邮件线程
    is_end = [False]
    report_thread = report.RepoterThread(data_p,is_end)
    report_thread.start()


    # 开始爬取二手房信息
    print_msg('数据库已有连接，开始爬取')

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

    # 爬取图片
    print_msg('开始爬取图片')
    thread_count = 18
    thread_list = list()
    for i in range(thread_count):
        thread_list.append(crawl.DownloadImgThread(data_p))
    for thread in thread_list:
        thread.start()
    for thread in thread_list:
        thread.join()

    is_end[0] = True
    data_p.release()


def run():
    if not os.path.exists('data'):
        os.mkdir('data')
    if not os.path.exists(config.IMG_DIR):
        os.mkdir(config.IMG_DIR)
    while True:
        time_object = time.localtime(time.time())
        year_month = str(time_object.tm_year) + '_' + str(time_object.tm_mon)
        file_name = 'data/'+year_month+'.json'
        if not os.path.exists(file_name):
            config_list = os.listdir('config')
            with open(file_name,'w')as f:
                f.write(json.dumps(config_list))
        while True:
            with open(file_name)as f:
                config_list = json.loads(f.read())
            if len(config_list) == 0:
                time.sleep(3600)
                break
            config_name = config_list[0]
            print_msg('开始爬取 ' + config_name)
            load(config_name,year_month)
            main()
            print_msg('结束爬取 ' + config_name)
            config_list.pop(0)
            with open(file_name,'w')as f:
                f.write(json.dumps(config_list))



if __name__ == '__main__':
    run()
