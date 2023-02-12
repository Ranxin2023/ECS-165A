from lstore.config import *
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    def has_capacity(self):
        if self.num_records < RECORD_PER_PAGE:
            return True
        else:
            return False
        
    def write(self, value):
        self.num_records += 1
        begin = (self.num_records - 1) * RECORD_SIZE 
        end = self.num_records * RECORD_SIZE
        self.data[begin:end] = value.to_bytes(RECORD_SIZE, 'little')
        
        

class PageRange:
    
    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.base_page_list = []
        self.tail_pages = []
        self.tail_pages.append(Page())
        for i in range(MAX_PAGE):
            self.base_page_list.append(Page())
            
    # def new_base_page(self):
    #     for i in range(self.num_columns):
    #         self.base_page_list.append(Page())   
    
    def new_tail_page(self):
        self.tail_pages.append(Page())
    
    def has_capacity(self):
        return self.base_page_list[-1].has_capacity()
        
        
    
    
