from lstore.config import *


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        return self.num_records < RECORD_PER_PAGE

    def write(self, value):
        # print("value in Page is: {}".format(value))
        # new_value=int(value)
        self.data[self.num_records * 8:(self.num_records + 1) * 8] = int(value).to_bytes(8, byteorder='big')
        self.num_records += 1

    def get_value(self, index):
        value = int.from_bytes(self.data[index * 8:(index + 1) * 8], 'big')
        return value

    def update(self, index, value):
        self.data[index * 8:(index + 1) * 8] = int(value).to_bytes(8, byteorder='big')

#need to redefine page, combine this one with old one
#I create this class to avoid changing any function in page.py
class MyPage:
    def __init__(self):
        self.num_records=0
        self.dirty = False
        self.pinned = False
        self.data = bytearray(4096)

    def has_capacity(self):
        return self.num_records < RECORD_PER_PAGE

    def update(self, index, value):
        while self.pinned == 1:
            continue
        self.dirty=1
        self.pinned=1
        self.data[index * 8:(index + 1) * 8] = int(value).to_bytes(8, byteorder='big')
        self.pinned=0

    def write(self, value):
        while self.pinned == 1:
            continue
        self.pinned = 1
        self.data[self.num_records * 8: (self.num_records + 1) * 8] = (value).to_bytes(8, byteorder='big')
        self.pinned = 0
        self.num_records += 1

    def get_value(self, index):
        while self.pinned == 1:
            continue
        self.pinned = 1
        value = self.data[index * 8: (index + 1) * 8]
        self.pinned = 0
        return value

    def set_dirty(self):
        self.dirty = True

class PageRange:

    def __init__(self):
        self.base_page_index = 0
        self.tail_page_index = 0
        self.base_page = [Page() for _ in range(MAX_PAGE)]
        self.tail_page = [Page()]

    def inc_base_page_index(self):
        self.base_page_index += 1

    def current_base_page(self):
        return self.base_page[self.base_page_index]

    def current_tail_page(self):
        return self.tail_page[self.tail_page_index]

    def add_tail_page(self):
        self.base_page.append(Page())
        self.base_page_index += 1

    def last_base_page(self):
        return self.base_page_index == MAX_PAGE - 1

    '''
    def get_base_idx(self):
        return self.base_page_index
    '''
