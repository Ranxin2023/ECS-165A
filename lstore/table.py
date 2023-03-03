from lstore.index import Index
from time import time
from lstore.config import *
from lstore.page import PageRange
from lstore.bufferpool import BufferPool
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
        self.page_range_list = [[PageRange() for _ in range(self.total_num_columns)]]
        self.page_range_index = 0
        self.page_directory = {}
        self.index = Index(self)
        self.num_records = 0
        self.buffer_pool = BufferPool()
        self.key_RID = {}
        
        
    def set_path(self, path):
        self.table_path = path
        self.buffer_pool.initial_path(self.table_path)

    def new_page_range(self):
        self.page_range_list.append([PageRange() for _ in range(self.num_columns + DEFAULT_PAGE)])
        self.page_range_index += 1

    # column is the insert data
    def base_write(self, columns):
        for i, value in enumerate(columns):
            page_range = self.page_range_list[self.page_range_index][i]
            if page_range.current_base_page() == None:
                page_range.create_base_page(page_range.base_page_index)
            page = page_range.current_base_page()

            # if is the last page in the page range
            if page_range.last_base_page():
                # check if the last page is full
                if not page.has_capacity():
                    # allocate another pagerange
                    self.new_page_range()
                    # get the current page
                    page_range = self.page_range_list[self.page_range_index][i]
                    if page_range.current_base_page() == None:
                        page_range.create_base_page(page_range.base_page_index)
                    page = page_range.current_base_page()
            # if isn't last page in the page range
            else:
                if not page.has_capacity():
                    # current page is full
                    page_range.inc_base_page_index()
                    if page_range.current_base_page() == None:
                        page_range.create_base_page(page_range.base_page_index)
                    page = page_range.current_base_page()
                    
            buffer_id = (self.name, "base", i, self.page_range_index, page_range.base_page_index)
            print(page_range.base_page_index)
            page = self.buffer_pool.get_page(buffer_id)
            # write in
            # print("value in baseWrite: {} {}".format(i, value))
            page.write(value)
            offset = page.num_records - 1
            self.buffer_pool.updata_pool(buffer_id, page)
            
            page_range = self.page_range_list[self.page_range_index][i]
            page_range.base_page[page_range.base_page_index] = page
            
            
        #write in index
        #print("column in baseWrite: {}".format(columns))
        self.index.push_index(columns)
        
        # write address into page directory
        rid = columns[0]
        address = [self.name, "base", self.page_range_index, page_range.base_page_index, offset]
        self.page_directory[rid] = address
        self.key_RID[columns[self.key_column + DEFAULT_PAGE]] = rid
        self.num_records += 1
        

    def tail_write(self, *columns):
        columns = list(columns)
        # print("column in baseWrite: {}".format(column))
        
        for i, value in enumerate(columns):
            page_range = self.page_range_list[self.page_range_index][i]
            if page_range.current_tail_page() == None:
                page_range.add_tail_page()
            page = page_range.current_tail_page()

            # check if page is full
            if not page.has_capacity():
                # current page is full
                page_range.add_tail_page()
                page = page_range.current_tail_page()
            buffer_id = (self.name, "tail", i, self.page_range_index, page_range.tail_page_index)
            page = self.buffer_pool.get_page(buffer_id)
            # write in
            # print("value in tail_write: {} {}".format(i, value))
            page.write(value)
            offset = page.num_records - 1
            self.buffer_pool.updata_pool(buffer_id, page)
            
        # write address into page directory
        rid = columns[0]
        address = [self.name, "tail", self.page_range_index, page_range.tail_page_index, offset]
        self.page_directory[rid] = address
        self.key_RID[columns[self.key_column + DEFAULT_PAGE]] = rid
        self.num_records += 1
        

    # pages is the given column that we are going to find the sid
    def find_value(self, column_index, location):
        # print("location")
        # print(location)
        buffer_id = (location[0], location[1], column_index, location[2], location[3])
        # print("buffer id")
        # print(buffer_id)
        page = self.buffer_pool.get_page(buffer_id)
        value = page.get_value(location[4])
        
        return value

    def update_value(self, column_index, location, value):
        buffer_id = (location[0], location[1], column_index, location[2], location[3])
        page = self.buffer_pool.get_page(buffer_id)
        page.update(location[4], value)
        self.buffer_pool.updata_pool(buffer_id, page)

    def find_record(self, rid):
        row = []
        
        for i in range(DEFAULT_PAGE + self.num_columns):
            location = self.page_directory[rid]
            result = self.find_value(i, location)
            row.append(result)
        return row

    def __merge(self):
        print("merge is happening")
        pass
