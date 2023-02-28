from lstore.LRU import LRU
from lstore.page import Page, MyPage
from lstore.config import *
import numpy as np
import os

class BufferPool:
    def __init__(self):
        self.path=None
        self.LRU = LRU()
        self.page_directories={}
        #self.popped_directories={}

    def add_pages(self, buffer_id):
        self.page_directories[buffer_id] = MyPage()
        self.page_directories[buffer_id].set_dirty()

    def is_full(self):
        return self.LRU.is_full()

    def is_page_in_buffer(self, buffer_id):
        return buffer_id in self.page_directories

    def uid_to_path(self, buffer_id):

        t_name, base_tail, column_id, page_range_id, page_id = buffer_id
        path = os.path.join(self.path, t_name, base_tail, str(column_id),str(page_range_id), str(page_id) + ".pkl")
        return path

    def write_page(self, columns, path):
        arr = np.zeros(shape=(len(columns), 7), dtype='int')
        # transfer to int
        for i in range(len(columns)):
            for j in range(len(columns[i])):
                # print(int.from_bytes(columns[i][j], "big"))
                arr[i][j] = int.from_bytes(columns[i][j], byteorder="big")
        # write
        dirname = os.path.dirname(path)
        print(dirname)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        f = open(path, "w")
        content = str(arr)
        f.write(content)
        f.close()

    def read_page(self, page_path):
        f = open(page_path, "rb")
        page = f.read()  # Load entire page object
        new_page = MyPage()
        new_page.from_file(page)
        f.close()
        return new_page



