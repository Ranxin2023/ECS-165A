from lstore.index import Index
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


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
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.num_tail = 0

    
        
        

    def table_initialize(self):
        # The pageRange is 16 with 4 default_page, schema_encoding, indirection...
        self.page_directory = {'base': []}
        self.page_directory['base'] = [[PageRange()] for _ in range(self.num_columns + DEFAULT_PAGE)]



    def add_tail(self):
        self.num_tail += 1
        self.page_directory['tail'+ str(self.num_tail)] = [[Page()] for _ in range(self.num_columns + DEFAULT_PAGE)]


    def if_tail_full(self):
        if not self.page_directory['tail'][-1].has_capacity():
            self.add_tail()
            

    # column is the insert data
    def baseWrite(self, column):

        for i, value in enumerate(column):
            pages = self.page_directory['base'][i][-1]
            page = pages.current_page()
            # if is the last page
            if pages.last_page():
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
                    page = pages.current_page()

        # write in
            page.write(value)


    # same as base write
    def tailWrite(self, column):
        for i, value in enumerate(column):
            pages = self.page_directory['base'][i][-1]
            page = pages.current_page()
            if not page.has_capacity():
                self.page_directory['base'][i][-1].append(PageRange())
                page = pages.current_page()
            page.write(column)


    def __merge(self):
        print("merge is happening")
        pass
 
