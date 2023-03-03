from datetime import datetime
from lstore.LRU import LRU
from lstore.page import Page, PageRange
from lstore.config import *
import numpy as np
import pickle
import os

class BufferPool:
    def __init__(self, capacity = 200):
        self.path = ""
        self.LRU = LRU()
        self.capcity = capacity
        self.pool = {}
        # self.tps = {}
        
        
    def initial_path(self, path):
        self.path = path

#     need for merge
    def initial_tps(self, t_name):
        if t_name not in self.tps.keys():
            self.tps[t_name] = {}
    
    
    def page_buffer_checker(self, buffer_id):
        return buffer_id in self.pool.keys()
            
    def add_pages(self, buffer_id, page):
        self.pool[buffer_id] = page
        self.pool[buffer_id].set_dirty()
        
    def updata_pool(self, buffer_id, page):
        self.pool[buffer_id] = page
        self.pool[buffer_id].set_dirty()

    def is_full(self):
        return self.LRU.is_full()

    def bufferid_path_pkl(self, buffer_id):
        # table_name, base_tail, page_range, column_page, page_index = buffer_id
        # path = os.path.join(self.path, str(column_page), str(page_range), base_tail, str(page_index) + '.pkl')
        
        dirname = os.path.join(self.path, str(buffer_id[2]), str(buffer_id[3]), buffer_id[1])
        file_path = os.path.join(dirname, str(buffer_id[4]) + '.pkl')
        return file_path
    
    
    # def bufferid_path_txt(self, buffer_id):
    #     table_name, base_tail, column_page, page_range, page_index = buffer_id
    #     path =  os.path.join(self.path, table_name, str(column_page), str(page_range), base_tail, str(page_index) + 'txt')
    #     return path

        
    def get_page(self, buffer_id):
        if buffer_id in self.pool:
            return self.pool[buffer_id]
        path = self.bufferid_path_pkl(buffer_id)
        # create new_page
        if not os.path.isfile(path):
            page = Page()
            self.add_pages(buffer_id, page)
            # dirname = os.path.dirname(path)
            # if not os.path.isdir(dirname):
            #     os.makedirs(dirname)
            return page
        # page already exist
        else:
            if not self.page_buffer_checker(buffer_id):
                self.pool[buffer_id] = self.read_page(path)
            self.LRU.put(buffer_id, datetime.timestamp(datetime.now()))
            return self.pool[buffer_id]
        

    def read_page(self, path):
        f = open(path, 'r+b')
        page = Page()
        metadata = pickle.load(f)
        
        page.num_records = metadata[0]
        page.dirty = metadata[1]
        page.pinned = metadata[2]
        page.tps = metadata[3]
        page.data = pickle.load(f)
        f.close()
        return page
        

    def write_page(self, page, buffer_id):
        dirname = os.path.join(self.path, str(buffer_id[2]), str(buffer_id[3]), buffer_id[1])
        file_path = os.path.join(dirname, str(buffer_id[4]) + '.pkl')
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        f = open(file_path, 'w+b')
        metadata = [page.num_records, page.dirty, page.pinned, page.tps]
        pickle.dump(metadata, f)
        pickle.dump(page.data, f)
        f.close()

    def close(self):
        for buffer_id in self.pool:
            self.write_page(self.pool[buffer_id], buffer_id)


