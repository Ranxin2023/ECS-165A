from lstore.table import Table
from lstore.bufferpool import *
import os

class Database():

    def __init__(self):
        self.path = ''
        self.tables = {}
        self.bufferpool = BufferPool()


    # Not required for milestone1
    def open(self, path):
        self.path = path
        if not os.path.exists(path):
            os.makedirs(path)
        self.bufferpool.initial_path(path)

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        self.tables[name] = table;
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name in self.tables.keys():
            del self.tables[name]
        else:
            print(f"table {name} does not exist in the tables")
        pass

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        if name in self.tables.keys():
            return self.tables[name]
            

