from lstore.config import *

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        return self.num_records < RECORD_PER_PAGE

    def write(self, value):
        #print("value in Page is: {}".format(value))
        new_value=int(value)
        self.data[self.num_records * 8:(self.num_records + 1) * 8] = new_value.to_bytes(8, byteorder='big')
        self.num_records += 1

    def get_value(self, index):
        return self.data[index * 8:(index + 1) * 8]


class PageRange():

    def __init__(self):
        self.base_page_index = 0
        self.num_tail = 0
        self.base_page_list = [Page() for _ in range(MAX_PAGE)]
        # self.tail_page_list = [Page()]

    def new_base_page(self):
        self.base_page_index += 1

    def new_tail_page(self):
        self.tail_page_list.append(Page())
        self.num_tail += 1

    def current_page(self):
        return self.base_page_list[self.base_page_index]

    def last_page(self):
        return self.base_page_index == MAX_PAGE-1