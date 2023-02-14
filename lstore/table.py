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
        self.page_directory = {}
        self.index = Index(self)
        self.num_records = 0
        self.table_initialize()
        self.key_RID = {}

    def table_initialize(self):
        # The pageRange is 16 with 4 default_page, schema_encoding, indirection...
        self.page_directory = {'base': []}
        self.page_directory['base'] = [[PageRange()] for _ in range(self.num_columns + DEFAULT_PAGE)]



    def add_tail(self):
        self.num_tail += 1
        self.page_directory['tail'+ str(self.num_tail)] = [Page() for _ in range(self.num_columns + DEFAULT_PAGE)]


    def if_tail_full(self):
        if not self.page_directory['tail'+ str(self.num_tail)].has_capacity():
            self.add_tail()
        #     return True
        # else:
        #     return False



    # pages is the given column that we are going to find the sid
    def find_value(self, pages, value):
        for i in range(pages):
            for j in range(pages[i].pages):
                for k in range(pages[i].pages[j].num_records):
                    if pages[i].pages[j].get_value(k) == value:
                        pages_number = i
                        locate_pageRange = j
                        index = k

                        return pages_number, locate_pageRange, index



    # column is the insert data
    def baseWrite(self, column):
        for i, value in enumerate(column):
            page_range = self.page_directory['base'][i][-1]
            page = page_range.current_page()
            # if is the last page
            if page_range.last_page():
                # check if the last page is full
                if not page.has_capacity():
                    # allocate another page
                    self.page_directory['base'][i].append(PageRange())
                    # get the current page
                    page = self.page_directory['base'][i][-1].get_current()
            # if isn't last page
            else:
                if not page.has_capacity():
                    # current page is full
                    self.page_directory['base'][i][-1].indexIncrement()
                    page = page_range.current_page()

        # write in
            page.write(value)


    # same as base write
    def tailWrite(self, column):
        for i, value in enumerate(column):
            page = self.page_directory['tail'+str(self.num_tail)][i]
            page.write(value)


    def get_tail_info(self, indirection):
        # find the tail page
        locate_tail_page = (indirection // RECORD_PER_PAGE) + 1
        tail_page = indirection % RECORD_PER_PAGE

        return locate_tail_page, tail_page

    def get_indirection(self, rid, locate_tail_page):

        return self.page_directory['tail'+str(locate_tail_page)][INDIRECTION].get(rid)

    def base_delete(self, rid):

        pass

    def __merge(self):
        print("merge is happening")
        pass
