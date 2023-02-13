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
      
    
    def get_value(self, index):
        
        return self.data[index*8:(index+1) * 8]
        
        

class PageRange:
    
    def __init__(self):
        self.base_page_list = []
        self.indexes = 0
        for i in range(MAX_PAGE):
            self.base_page_list.append(Page())
            
    def indexIncrement(self):
        self.indexes += 1

    def current_page(self):
        return self.base_page_list[self.indexes]

    def last_page(self):
        return self.indexes == 15
        
        
    
    
