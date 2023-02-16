from lstore.config import *


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        return self.num_records < RECORD_PER_PAGE

    def write(self, value):
        #print("value in Page is: {}".format(value))
        #new_value=int(value)
        self.data[self.num_records * 8:(self.num_records + 1) * 8] = int(value).to_bytes(8, byteorder='big')
        self.num_records += 1

    def get_value(self, index):
        return self.data[index * 8:(index + 1) * 8]
    
    def update(self, index, value):
        self.data[index * 8:(index + 1) * 8] = value.to_bytes(8, byteorder='big')


class PageRange():

    def __init__(self):
        self.indexes = 0
        self.num_tail = 0
        self.pages = [Page() for _ in range(MAX_PAGE)]

    def index_increment(self):
        self.indexes += 1

    def current_page(self):
        return self.pages[self.indexes]

    def last_page(self):
        return self.indexes == MAX_PAGE-1
    '''
    def get_base_idx(self):
        return self.base_page_index
    '''
