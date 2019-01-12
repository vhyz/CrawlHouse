import pymysql
from queue import Queue
import threading
import config


class MysqlPool():

    def __init__(self, init_count=15):
        self.conn_queue = Queue()
        self.lock = threading.Lock()
        for i in range(init_count):
            self.conn_queue.put(self.create_conn())

    def create_conn(self):
        conn = pymysql.connect('localhost', config.MYSQL_NAME, config.MYSQL_PASSWORD, config.DATABASE_NAME)
        return conn

    def release(self):
        while not self.conn_queue.empty():
            conn = self.conn_queue.get()
            conn.close()

    def get(self):
        if self.lock.acquire():
            if self.conn_queue.empty():
                conn = self.create_conn()
            else:
                conn = self.conn_queue.get()
            self.lock.release()
            return conn

    def free(self, conn):
        self.conn_queue.put(conn)
