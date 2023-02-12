from lstore.table import Table, Record
from lstore.index import Index
from config import *

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        pass
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        schema_encoding = 0
        indirection = MAX_INT
        rid = self.table.num_records
        time = datetime.now().strftime("%Y%m%d%H%M%S")
        meta_data = [rid, time, schema_encoding, indirection]
        columns = list(columns)
        meta_data.extend(columns)

        self.table.baseWrite(meta_data)

        self.table.num_records += 1
        
        return True
    
    
    def delete(self, primary_key):
        
        # need to complete get_base_record,get_key_indirection,update_record_values,get_tail_record,
        # delete_rid_tail,update_record_values,num_updates,num_records in table.py
        
    base_record = self.table.get_base_record(primary_key) 
    if base_record is None:
        raise ValueError(f"Record with primary key {primary_key} does not exist.")
    self.table.delete_rid_base(*base_record)

    indirection_base_bytes = self.table.get_key_indirection(primary_key)
    indirection_base_int = int.from_bytes(indirection_base_bytes, byteorder='big')

    if indirection_base_int == MAXINT:
        self.table.update_record_values(*base_record, 0)
    else:
        tail_record = self.table.get_tail_record(indirection_base_int)
        if tail_record is None:
            raise ValueError(f"Tail record for primary key {primary_key} does not exist.")
        self.table.delete_rid_tail(*tail_record)
        self.table.update_record_values(*tail_record, 0)

        self.table.num_updates -= 1
    self.table.num_records -= 1
    
    def insert_record(self, *columns):
        
        # need to complete num_records,num_columns,write_to_base,key_list in table.py
        
    indirection = MAXINT
    rid = self.table.num_records
    curr_time = int(time.time())
    schema_encoding = '0' * self.table.num_columns
    schema_encoding = int.from_bytes(schema_encoding.encode(), byteorder='big')

    default_columns = [indirection, rid, curr_time, schema_encoding]
    all_columns = default_columns + list(columns)
    try:
        self.table.write_to_base(all_columns)
        self.table.key_list.append(columns[self.table.key_index])
        self.table.num_records += 1
        return True
    except:
        return False



    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        pass

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        pass

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        pass

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
