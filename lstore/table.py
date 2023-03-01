from lstore.index import Index
from time import time
from lstore.config import *
from lstore.page import PageRange, Page


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
        self.name = name
        self.key_column = key_column

        self.num_columns = num_columns
        self.total_num_columns = self.num_columns+DEFAULT_PAGE
        #print("{}".format(self.num_columns))
        self.page_directory = {}
        self.index = Index(self)
        self.num_records = 0
        self.page_range_list = [[PageRange() for _ in range(self.total_num_columns)]]
        self.page_range_index = 0
        self.key_RID = {}


    def new_page_range(self):
        self.page_range_list.append([PageRange() for _ in range(self.num_columns + DEFAULT_PAGE)])
        self.page_range_index += 1

    # column is the insert data
    def locate_page_range_index(self):
        locate_page_range = self.num_records // RECORD_PER_PAGE // MAX_PAGE
        page_index = (self.num_records // RECORD_PER_PAGE) % MAX_PAGE

        return locate_page_range, page_index

    # column is the insert data
    def base_write(self, columns):
        # print("column in baseWrite: {}".format(column))
        for i, value in enumerate(columns):
            locate_page_range, page_index = self.locate_page_range_index()
            page_info = [self.name, 'Base', i, locate_page_range, page_index]
            page_range = self.page_range_list[self.page_range_index][i]
            page_from_memory = page_range.current_base_page()
            page_from_buffer = self.bufferpool.get_page(*page_info)
            buffer_id = tuple(page_info)

            buffer_path_pickle = self.bufferpool.bufferid_path_pkl(buffer_id)
            self.bufferpool.write_page(page_from_buffer, buffer_path_pickle)

            # print("page range num:{}".format(len(self.page_range_list['base'][i])-1))
            # print("page number:{}".format(page_range.get_base_idx()))

            # if is the last page in the page range
            if page_range.last_base_page():
                # check if the last page is full
                if not page_from_memory.has_capacity() and not page_from_buffer.has.capacity():
                    # allocate another pagerange
                    self.new_page_range()
                    # get the current page
                    page_range = self.page_range_list[self.page_range_index][i]
                    page_range_index = self.page_range_index
                    page_info[-2] = page_range_index
                    page_info[-1] = 0
                    page_from_memory = page_range.current_base_page()
                    page_from_buffer = self.bufferpool.get_page(*page_info)

            # if isn't last page in the page range
            else:
                if not page_from_memory.has_capacity() and not page_from_buffer.has_capacity():
                    # current page is full
                    page_range.inc_base_page_index()
                    page_from_memory = page_range.current_base_page()
                    page_info[-1] += 1
                    page_from_buffer = self.bufferpool.get_page(*page_info)
            # write in
            # print("value in baseWrite: {} {}".format(i, value))

            page_from_memory.write(value)


            self.bufferpool.write_page(page_from_buffer, buffer_path_pickle)

            buffer_path = self.bufferpool.bufferid_path_txt(buffer_id)

            self.bufferpool.disk_write_page(buffer_path, value)
            page_from_memory.dirty = 1
            page_from_buffer.dirty = 1

        # write address into page directory
        rid = columns[self.key_column]
        address = ["base", self.page_range_index, page_range.base_page_index, page_from_memory.num_records - 1]
        self.page_directory[rid] = address

    def tail_write(self, columns):
        # print("column in baseWrite: {}".format(column))
        for i, value in enumerate(columns):
            page_range = self.page_range_list[self.page_range_index][i]
            page = page_range.current_tail_page()

            # print("page range num:{}".format(len(self.page_range_list['base'][i])-1))
            # print("page number:{}".format(page_range.get_base_idx()))

            # check if page is full
            if not page.has_capacity():
                # current page is full
                page_range.add_tail_page()
                page = page_range.current_tail_page()
            # write in
            # print("value in tail_write: {} {}".format(i, value))
            page.write(value)
        # write address into page directory
        rid = columns[self.key_column]
        address = ["tail", self.page_range_index, page_range.tail_page_index, page.num_records - 1]
        self.page_directory[rid] = address

    # pages is the given column that we are going to find the sid
    def find_value(self, column_index, location):
        page_range = self.page_range_list[location[1]][column_index]
        if location[0] == "base":
            page = page_range.base_page[location[2]]
        elif location[0] == "tail":
            page = page_range.tail_page[location[2]]

        value = page.get_value(location[3])
        return value

    def update_value(self, column_index, location, value):
        page_range = self.page_range_list[location[1]][column_index]
        if location[0] == "base":
            page = page_range.base_page[location[2]]
        elif location[0] == "tail":
            page = page_range.tail_page[location[2]]
        page.update(location[3], value)

    def find_record(self, rid):
        row = []
        location = self.page_directory[rid]
        for i in range(DEFAULT_PAGE + self.num_columns):
            result = self.find_value(i, location)
            row.append(result)
        return row

    def __merge(self):
        print("merge is happening")
        pass
