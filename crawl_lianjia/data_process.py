import traceback
import logging
import threading
import config
import pymysql

class Img:

    def __init__(self, url, id, name):
        self.url = url
        self.id = id
        self.name = name


class DataProcess():

    def __init__(self):
        self.conn_url = pymysql.connect('localhost', config.MYSQL_NAME, config.MYSQL_PASSWORD, config.DATABASE_NAME)
        self.conn_img = pymysql.connect('localhost', config.MYSQL_NAME, config.MYSQL_PASSWORD, config.DATABASE_NAME)
        self.url_lock = threading.Lock()
        self.img_lock = threading.Lock()
        self.img_table = config.HOUSE_TABLE + '_img'
        self.url_table = config.HOUSE_TABLE + '_url'

    def get_house_url(self):
        if self.url_lock.acquire(timeout=30):
            c = self.conn_url.cursor()
            try:
                c.execute('select * from {} limit 1  '.format(self.url_table))
                f = c.fetchone()
                if f is None:
                    self.conn_url.commit()
                    c.close()
                    self.url_lock.release()
                    return ''
                c.execute('delete from {} where id = '.format(self.url_table) + str(f[0]))
                self.conn_url.commit()
            except:
                logging.exception(traceback.format_exc())
            c.close()
            self.url_lock.release()
        return f[1]

    def get_img(self):
        if self.img_lock.acquire(timeout=30):
            c = self.conn_img.cursor()
            try:
                c.execute('select * from {} limit 10  '.format(self.img_table))
                f = c.fetchall()
                if len(f) == 0:
                    self.conn_img.commit()
                    c.close()
                    self.img_lock.release()
                    return list()
                for i in range(len(f)):
                    c.execute('delete from {} where id = '.format(self.img_table) + str(f[i][0]))
                self.conn_img.commit()
            except:
                logging.exception(traceback.format_exc())
            c.close()
            self.img_lock.release()
        return f

    def insert_img_url(self, img_list):
        if self.img_lock.acquire(timeout=30):
            c = self.conn_img.cursor()
            try:
                sql = 'Insert into {} (url, house_id, name) VALUES ("{}", "{}","{}") '.format(self.img_table, img_list[0].url,
                                                                                               str(img_list[0].id),
                                                                                               img_list[
                                                                                                   0].name + '.jpg')
                for i in range(1, len(img_list)):
                    sql += ',("{}", "{}","{}") '.format(img_list[i].url, str(img_list[i].id),
                                                        img_list[i].name + '.jpg')
                c.execute(sql)
                self.conn_img.commit()
            except:
                logging.exception(traceback.format_exc())
            c.close()
            self.img_lock.release()

    def create_table(self):
        c = self.conn_img.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY auto_increment , url VARCHAR(60))'.format(self.url_table))
        c.execute('CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY auto_increment , url TEXT, house_id VARCHAR(15), name VARCHAR(15))'.format(self.img_table))
        create_house_table_sql = """CREATE TABLE IF NOT EXISTS {}(
                                id BIGINT PRIMARY KEY,
                                title VARCHAR(50),
                                price VARCHAR(20),
                                favor_count VARCHAR(10),
                                unit_price VARCHAR(20),
                                location VARCHAR(50),
                                base_info_1 VARCHAR(15),
                                base_info_2 VARCHAR(15),
                                base_info_3 VARCHAR(10),
                                base_info_4 VARCHAR(10),
                                base_info_5 VARCHAR(10),
                                base_info_6 VARCHAR(10),
                                base_info_7 VARCHAR(10),
                                base_info_8 VARCHAR(10),
                                base_info_9 VARCHAR(10),
                                base_info_10 VARCHAR(10),
                                base_info_11 VARCHAR(10),
                                base_info_12 VARCHAR(10),
                                sell_info_1 VARCHAR(15),
                                sell_info_2 VARCHAR(10),
                                sell_info_3 VARCHAR(15),
                                sell_info_4 VARCHAR(10),
                                sell_info_5 VARCHAR(10),
                                sell_info_6 VARCHAR(10),
                                sell_info_7 VARCHAR(10),
                                sell_info_8 VARCHAR(10),
                                intro TEXT,
                                layout TEXT,
                                count_7 VARCHAR(10),
                                count_30 VARCHAR(10),
                                jingweidu VARCHAR(50),
                                comment TEXT,
                                community_id VARCHAR(20) )
                              """.format(config.HOUSE_TABLE)
        create_community_table_sql = """CREATE TABLE IF NOT EXISTS {}(
                                id BIGINT PRIMARY KEY,
                                community_name VARCHAR(20),
                                favor_count VARCHAR(10),
                                unit_price VARCHAR(20),
                                build_time VARCHAR(10),
                                buliding_type VARCHAR(30),
                                property_fee VARCHAR(30),
                                property_company VARCHAR(30),
                                developer VARCHAR(30),
                                building_count VARCHAR(10),
                                house_count VARCHAR(10),
                                nearby_stores VARCHAR(50) )
                              """.format(config.COMMUNITY_TABLE)
        c.execute(create_community_table_sql)
        c.execute(create_house_table_sql)
        c.close()
        self.conn_img.commit()

    def house_url_count(self):
        c = self.conn_url.cursor()
        c.execute('select count(*) from {}'.format(self.url_table))
        f = c.fetchone()
        c.close()
        return f[0]

    def img_url_count(self):
        c = self.conn_img.cursor()
        c.execute('select count(*) from {}'.format(self.img_table))
        f = c.fetchone()
        c.close()
        return f[0]

    def insert_house_url_set(self, url_set):
        url_list = list()
        c = self.conn_url.cursor()
        count = 0
        url_set_len = len(url_set)
        for url in url_set:
            url_list.append(url)
            count += 1
            if len(url_list) == 1000 or count == url_set_len:
                if len(url_list) == 0:
                    continue
                try:
                    sql = 'Insert into {} (url) VALUES ("{}") '.format(self.url_table, url_list[0])
                    for i in range(1, len(url_list)):
                        sql += ',("{}")'.format(url_list[i])
                    c.execute(sql)
                    self.conn_url.commit()
                except:
                    logging.exception(traceback.format_exc())
                url_list.clear()

    def release(self):
        self.conn_img.close()
        self.conn_url.close()
