from lstore.table import Table
from lstore.bufferpool import BufferPool
import os
import pickle



class Database():

    def __init__(self):
        self.tables = {}
        self.db_path = ""
        pass

    # Not required for milestone1
    def open(self, path):
        self.db_path = path
        
        if not os.path.exists(path):
            os.makedirs(path)
        else:
            path = os.path.join(self.db_path, "db_metadata.pkl")
            file = open(path, 'r+b')
            t_meta = pickle.load(file)
            file.close()
            for name in t_meta:
                metadata = t_meta[name]
                table = self.create_table(metadata[0], metadata[1], metadata[2])
                table.page_directory = metadata[3]
                table.num_records = metadata[4]
                table.page_range_index = metadata[5]
                table.key_RID = metadata[6]
                table.table_path = self.db_path
        
        

    def close(self):
        t_meta = {}
        for table in self.tables.values():
            t_meta[table.name] = [table.name, table.num_columns, table.key_column, table.page_directory, table.num_records]
            t_meta[table.name].append(table.page_range_index)
            t_meta[table.name].append(table.key_RID)
            
        path = os.path.join(self.db_path, "db_metadata.pkl")
        file = open(path, 'w+b')
        pickle.dump(t_meta, file)
        file.close()
        for table in self.tables.values():
            buffer_pool = table.buffer_pool
            buffer_pool.close()
        
        

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        if name in self.tables.keys():
            print(f"table {name} already exist in the database")
        else:
            table = Table(name, num_columns, key_index)
            table.set_path(os.path.join(self.db_path, table.name))
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
        else:
            print(f"table {name} does not exist in the database")
            

