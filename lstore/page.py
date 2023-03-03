from lstore.config import *


        
class Page:

    def __init__(self):
        self.num_records = 0
        self.dirty = False
        self.data = bytearray(4096)
        self.tps = 0
        self.pinned = 0

    def has_capacity(self):
        return self.num_records < RECORD_PER_PAGE

    def write(self, value):
        # print("value in Page is: {}".format(value))
        # new_value=int(value)
        self.data[self.num_records * 8:(self.num_records + 1) * 8] = int(value).to_bytes(8, byteorder='big')
        self.num_records += 1
        
    def set_dirty(self):
        self.dirty = True

    def get_value(self, index):
        value = int.from_bytes(self.data[index * 8:(index + 1) * 8], 'big')
        return value
    
    def find_value(self, target):
        offsets = []
        for i in range(RECORD_PER_PAGE):
            value = int.from_bytes(self.data[i * 8:(i + 1) * 8], 'big')
            if value == target:
                offsets.append(i)
        return offsets        

    def update(self, index, value):
        self.data[index * 8:(index + 1) * 8] = int(value).to_bytes(8, byteorder='big')


class PageRange:

    def __init__(self):
        self.base_page_index = 0 
        self.tail_page_index = 0 
        self.base_page = [None for _ in range(MAX_PAGE)]
        self.tail_page = [None] 
        
    def is_page_exist(self, index, type):
        if type == "base":
            return self.base_page[index] != None
        else:
            return self.tail_page[index] != None
        
    # content will be a page object(read from disk) if passed in    
    def create_base_page(self, index, content = None): 
        if content == None:
            self.base_page[index] = Page()
        else:
            self.base_page[index] = content

    def inc_base_page_index(self):
        self.base_page_index += 1

    def current_base_page(self):
        return self.base_page[self.base_page_index]

    def current_tail_page(self):
        return self.tail_page[self.tail_page_index]

    def add_tail_page(self):
        if self.tail_page[self.tail_page_index] == None:
            self.tail_page[self.tail_page_index] = Page()
        else:
            self.tail_page.append(Page())
            self.tail_page_index += 1
        

    def last_base_page(self):
        return self.base_page_index == MAX_PAGE - 1

    '''
    def get_base_idx(self):
        return self.base_page_index
    '''
