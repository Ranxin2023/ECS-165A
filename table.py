from lstore.index import Index
from lstore.config import *
from lstore.page import PageRange
from lstore.bufferpool import BufferPool
import time
import threading
import os


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns


class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key_column):
        self.table_path = ""
        self.name = name
        self.key_column = key_column
        self.num_columns = num_columns
        self.total_num_columns = self.num_columns+DEFAULT_PAGE
        self.page_directory = {}
        self.index = Index(self)
        self.num_records = 0
        self.num_updates = 0
        self.key_RID = {}
        
        
    def set_path(self, path):
        self.table_path = path
        BufferPool.initial_path(self.table_path)

    # def new_page_range(self):
    #     self.page_range_list.append([PageRange() for _ in range(self.num_columns + DEFAULT_PAGE)])
    #     self.page_range_index += 1
        
    # def merge_trigger(self, buffer_id):
    #     if self.num_updates % RECORD_PER_PAGE == 0:
    #         # make copy of base pages before merge start
            
    #         t = threading.Thread(target = self.__merge(), daemon = True)
    #         t.start()
    
    # def __merge(self):
    #     print("merge is happening")
        
    #     pass

    # using a number to find the location of the page 
    def determine_page_location(self, type):
        num_tail = self.num_updates
        num_base = self.num_records - num_tail
        page_range_index = num_base % RECORD_PER_PAGERANGE
        if type == 'base':
            base_page_index = num_base % RECORD_PER_PAGE
            return page_range_index, base_page_index
        else:
            tail_page_index = num_tail % RECORD_PER_PAGE
            return page_range_index, tail_page_index

    # column is the insert data
    def base_write(self, columns):
        page_range_index, base_page_index = self.determine_page_location('base')
        for i, value in enumerate(columns):     
            # use buffer_id to find the page
            buffer_id = (self.name, "base", i, page_range_index, base_page_index)
            page = BufferPool.get_page(buffer_id)
            
            if not page.has_capacity():
                if base_page_index == MAX_PAGE - 1:
                    page_range_index += 1
                    base_page_index = 0
                else:
                    base_page_index += 1
            
            # write in
            page.write(value)
            offset = page.num_records - 1
            BufferPool.updata_pool(buffer_id, page)  
        
        # write address into page directory
        rid = columns[0]
        address = [self.name, "base", page_range_index, base_page_index, offset]
        self.page_directory[rid] = address
        self.key_RID[columns[self.key_column + DEFAULT_PAGE]] = rid
        self.num_records += 1
        self.index.push_index(columns[DEFAULT_PAGE:len(columns) + 1], rid)
        

    def tail_write(self, columns):
        page_range_index, tail_page_index = self.determine_page_location('tail')
        # print("column in baseWrite: {}".format(column))
        for i, value in enumerate(columns):
            # use buffer_id to find the page
            buffer_id = (self.name, "tail", i, page_range_index,tail_page_index)
            page = BufferPool.get_page(buffer_id)
            
            if not page.has_capacity():
                if base_page_index == MAX_PAGE - 1:
                    page_range_index += 1
                    base_page_index = 0
                else:
                    base_page_index += 1
                    
            page = BufferPool.get_page(buffer_id)
            # write in
            # print("value in tail_write: {} {}".format(i, value))
            page.write(value)
            offset = page.num_records - 1
            BufferPool.updata_pool(buffer_id, page)
            
        # write address into page directory
        rid = columns[0]
        address = [self.name, "tail", page_range_index, tail_page_index, offset]
        self.page_directory[rid] = address
        self.key_RID[columns[self.key_column + DEFAULT_PAGE]] = rid
        self.num_records += 1
        self.num_updates += 1
        
    def find_value_rid(self, column_index, target):
        rids = []
        for rid in self.page_directory:
            record = self.find_record(rid)
            if record[column_index + DEFAULT_PAGE] == target:
                rids.append(rid)
        return rids

    # pages is the given column that we are going to find the sid
    def find_value(self, column_index, location):
        buffer_id = (location[0], location[1], column_index, location[2], location[3])
        page = BufferPool.get_page(buffer_id)
        value = page.get_value(location[4])
        return value

    def update_value(self, column_index, location, value):
        buffer_id = (location[0], location[1], column_index, location[2], location[3])
        page = BufferPool.get_page(buffer_id)
        page.update(location[4], value)
        BufferPool.updata_pool(buffer_id, page)

    def find_record(self, rid):
        row = []
        location = self.page_directory[rid]
        for i in range(DEFAULT_PAGE + self.num_columns):
            result = self.find_value(i, location)
            row.append(result)
        return row
