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
        pass
    
    
    def table_initialize(self, ):
        self.page_directory = {'base': [], 'tail': ''}
        for i in range(self.num_columns + defalut_page):
            self.page_directory['base'] = [[PageRange()] for j in range(self.num_columns + defalut_page)]
            self.page_directory['tail'] = [[Page()] for j in range(self.num_columns + defalut_page)]


    def write_table(self, data):


        # if the page is not full



        # if the page is full
        pass

    def rid_record(self, rid):

        pass

    def schema_encoding(self, column):

        pass

    def __merge(self):
        print("merge is happening")
        pass
 
