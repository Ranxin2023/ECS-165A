from datetime import datetime
from lstore.LRU import LRU
from lstore.page import Page, MyPage
from lstore.config import *
import numpy as np
import pickle
import os

class BufferPool:
    def __init__(self):
        self.path="./ECS165"
        self.LRU = LRU()
        self.page_directories={}
        self.tps = {}
        #self.popped_directories={}
        
    def initial_path(self, path):
        self.path = path

#     need for merge
    def initial_tps(self, t_name):
        if t_name not in self.tps.keys():
            self.tps[t_name] = {}
            
            
    def add_pages(self, buffer_id):
        self.page_directories[buffer_id] = MyPage()
        self.page_directories[buffer_id].set_dirty()

    def is_full(self):
        return self.LRU.is_full()

    def page_buffer_checker(self, buffer_id):
        return buffer_id in self.page_directories

    def bufferid_path_pkl(self, buffer_id):
        table_name, base_tail, page_range, column_page, page_index = buffer_id
        path =  os.path.join(self.path, table_name, base_tail, str(column_page), str(page_range), str(page_index) + 'pkl')
        return path
    
    
    def bufferid_path_txt(self, buffer_id):
        table_name, base_tail, page_range, column_page, page_index = buffer_id
        path =  os.path.join(self.path, table_name, base_tail, str(column_page), str(page_range), str(page_index) + 'txt')
        return path

   
        
    def get_page(self, table_name, base_tail, column_page, page_range, page_index):
        buffer_id = (table_name, base_tail, column_page, page_range, page_index)
        path = self.bufferid_path_pkl(buffer_id)
        # create new_page
        if not os.path.isfile(path):
            self.add_page(buffer_id, default=False)
            dirname = os.path.dirname(path)
            if not os.path.isdir(dirname):
                os.makedirs(dirname)
            f = open(path, "w+")
            f.close()
        #     page already exist
        else:
            if not self.page_buffer_checker(buffer_id):
                self.page_directories[buffer_id] = self.read_page(path)
                
            self.LRU.put(buffer_id, datetime.timestamp(datetime.now()))
            return self.page_directories[buffer_id]
        

    def read_page(self, path):
        f = open(path, 'rb')
        page = pickle.load(f)
        new_page = Page()
        new_page.num_records = page.num_records
        new_page.data = page.data
        f.close()
        return new_page

    def write_page(self, page, path):
        dirname = os.path.dirname(path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        f = open(path, 'wb')
        pickle.dump(page, f)
        f.close()




