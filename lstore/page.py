
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        if self.num_records < 512:
            return True
        else:
            return False
        
    def write(self, value):
        begin = self.num_records * 8
        end = (self.num_records + 1) * 8
        self.data[begin:end] = value.to_bytes(8, 'little')
        self.num_records += 1
        pass

