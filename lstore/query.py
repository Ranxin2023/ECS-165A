from lstore.table import Table, Record
from lstore.index import Index
from datetime import datetime
from lstore.config import *





class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        self.index = Index(table)

    """
    # internal Method
    # Read a record with specified key
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, key):
        rid = self.table.key_RID[key]
        
        del self.table.key_RID[key]
        del self.table.page_directory[rid]

        return True

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        # 加【key，RID】进去table.key_RID
        key = columns[self.table.key_column]
        if key in self.table.key_RID.keys():
            return False;
        
        schema_encoding = '0' * self.table.num_columns
        indirection = MAX_INT
        rid = self.table.num_records
        time = datetime.now().strftime("%Y%m%d%H%M%S")
        meta_data = [rid, int(time), schema_encoding, indirection]
        columns = list(columns)
        # print("columns in insert: {}".format(columns))
        meta_data.extend(columns)
        # print("metadata in insert: {}".format(meta_data))
        
        self.table.base_write(meta_data)

        return True

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def find_column_bufferID(self, column):
        buffer_rid = []
        num_page_range = self.table.page_range_index + 1
        for i in range(num_page_range):
            for k in range(self.table.page_range_list[i].base_page_index):
                buffer_id = (self.table.table_path, "base", str(column), str(i), str(k))
                buffer_rid.append(buffer_id) 
        
        return buffer_rid
    
    
    def select(self, search_key, search_key_index, projected_columns_index):
        records = []
        column = []
        rids = []
        
        index_column = search_key_index + DEFAULT_PAGE
        if search_key_index == self.table.key_column:
            rid = self.table.key_RID[search_key]
            rids.append(rid)
        else:
            rids = self.find_column_bufferID(index_column)
        
        for rid in rids:
            result = self.table.find_record(rid)
            # print(result)
            column = result[DEFAULT_PAGE:DEFAULT_PAGE + self.table.num_columns + 1]
        
            # if record has update record
            if result[INDIRECTION] != MAX_INT:
            # use indirection of base record to find tail record
                rid_tail = result[INDIRECTION]
                # rid_tail = self.table.key_RID[indirection]
                result_tail = self.table.find_record(rid_tail)
                updated_column = result_tail[DEFAULT_PAGE:DEFAULT_PAGE + self.table.num_columns + 1]
                encoding = result[SCHEMA_ENCODING]
                encoding = self.find_changed_col(encoding)
                for i, value in enumerate(encoding):
                    if value == 1:
                        column[i] = updated_column[i]
        
            # take columns that is requested
            for i in range(self.table.num_columns):
                if projected_columns_index[i] == 0:
                    column[i] = None
            record = Record(rid, search_key, column)
            records.append(record)
            
        return records

    # helper function to find which columns have updated
    def find_changed_col(self, encoding):
        count = self.table.num_columns
        result = [0 for _ in range(count)]
        if encoding == 0:
            return result
        while encoding != 0:
            if encoding % 10 == 1:
                result[count - 1] = 1
            encoding = encoding // 10
            count -= 1
        return result
        
    
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
        columns = list(columns)
        
        if primary_key not in self.table.key_RID.keys():
            return False;
        if columns[self.table.key_column] in self.table.key_RID.keys():
            return False;
        
        tail_rid = self.table.num_records
        time = datetime.now().strftime("%Y%m%d%H%M%S")
        
        rid = self.table.key_RID[primary_key]
        result = self.table.find_record(rid)
        indirection = result[INDIRECTION]
        new_encoding = '' 
        location_base = self.table.page_directory[rid]
        
        # first time update
        if indirection == MAX_INT:
            tail_indirect = rid
            for i, value in enumerate(columns):
                if value == None:
                    new_encoding += '0'
                    columns[i] = MAX_INT
                else:
                    new_encoding += '1'
        else:
            latest_tail = self.table.find_record(indirection)
            tail_indirect = latest_tail[RID]
            encoding = latest_tail[SCHEMA_ENCODING]
            encoding = self.find_changed_col(encoding)
            for i, value in enumerate(encoding):
                if columns[i] != None:
                    new_encoding += '1'
                else:
                    columns[i] = latest_tail[i + DEFAULT_PAGE]
                    if latest_tail[i + DEFAULT_PAGE] != MAX_INT:
                        new_encoding += '1'
                    else:
                        new_encoding += '0'
        
        # update base record
        self.table.update_value(INDIRECTION, location_base, tail_rid)
        self.table.update_value(SCHEMA_ENCODING, location_base, new_encoding)
        
        meta_data = [tail_rid, int(time), new_encoding, tail_indirect]
        
        # print("columns in insert: {}".format(columns))
        meta_data.extend(columns)
        
        self.table.tail_write(*meta_data)
        
        return True

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        total_sum = 0
        column_index = aggregate_column_index + DEFAULT_PAGE
        for key in range(start_range, end_range + 1):
            if key in self.table.key_RID.keys():
                rid = self.table.key_RID[key]
                record = self.table.find_record(rid)
                # never been updated
                if record[INDIRECTION] == MAX_INT:
                    total_sum += record[column_index]
                else:
                # have been updated
                    tail_rid = record[INDIRECTION]
                    tail_record = self.table.find_record(tail_rid)
                    updated_column = tail_record[DEFAULT_PAGE:DEFAULT_PAGE + self.table.num_columns + 1]
                    encoding = tail_record[SCHEMA_ENCODING]
                    encoding = self.find_changed_col(encoding)
                    # print(updated_column)
                    # print(tail_record)
                    if encoding[aggregate_column_index] == 1:
                        total_sum += updated_column[aggregate_column_index]
                    else:
                        total_sum += record[column_index]
                  
                    # if encoding[aggregate_column_index] == 0:
                    #     total_sum += record[column_index]
                    # else:
                    #     tail_address = self.table.page_directory[tail_rid]
                    #     result = self.table.find_value(column_index, tail_address)
                    #     if result != MAX_INT:
                    #         total_sum += result

        return total_sum

    
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
