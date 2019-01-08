import sqlite3
from queue import Queue
import threading


class Sqlite3Pool():

    def __init__(self, db, init_count=10):
        self.conn_queue = Queue()
        self.db = db
        self.lock = threading.Lock()
        for i in range(init_count):
            self.conn_queue.put(self.create_conn())

    def create_conn(self):
        conn = sqlite3.connect(self.db, check_same_thread=False)
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


