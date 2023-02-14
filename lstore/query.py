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
        schema_encoding = '0' * self.table.num_columns
        indirection = MAX_INT - 1
        rid = self.table.num_records
        time = datetime.now().strftime("%Y%m%d%H%M%S")
        meta_data = [rid, int(time), schema_encoding, indirection]
        columns = list(columns)
        meta_data.extend(columns)

        self.table.base_write(meta_data)
        
        # 加【key，RID】进去table.key_RID
        
        self.table.num_records += 1

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
    def select(self, search_key, search_key_index, projected_columns_index):
        # get the request column
        # search_key will be the student ID base on the m1_tester
        # Process: go to the base page and find if the data is updated,
        # if updated find the corresponding tail page and return the records
        pages = self.table.page_directory['base'][DEFAULT_PAGE + search_key_index]
        record = []
        records = []
        SID = search_key.to_bytes(8, byteorder='big')
        #  find the SID from the base page

        # using the b tree to indexing later on
        pages_number, locate_pageRange, index = self.table.find_value(pages, SID)

        indirection = self.table.page_directory['base'][INDIRECTION][pages_number].pages[locate_pageRange].get_value(index)
        indirection = int.from_bytes(bytes(indirection), byteorder='big')
        # using the b tree to indexing later on
        rid = self.table.page_directory['base'][RID][pages_number].pages[locate_pageRange].get_value(index)
        # if the record was modify
        # if the page was modify

        if indirection != MAX_INT:
            locate_tail_page, tail_page = self.table.get_tail_info(indirection)

            for i in range(DEFAULT_PAGE, DEFAULT_PAGE + self.table.num_columns):
                value = self.table.page_directory['tail' + str(locate_tail_page)][i].get_value(tail_page)
                record.append(value)

        # if the record was not modify
        else:
            for i in range(DEFAULT_PAGE, DEFAULT_PAGE + self.table.num_columns):
                value = self.table.page_directory['base'][i][pages_number].pages[locate_pageRange].get_value(index)
                record.append(value)

        for col, data in enumerate(projected_columns_index):
            if data == 0:
                record[col] = None
            else:
                # convert to int
                record[col] = int.from_bytes(bytes(record[col]), byteorder='big')

        key = record[self.table.key]
        rid = int.from_bytes(bytes(rid), byteorder='big')
        record_class = Record(rid, key, record)
        records.append(record_class)

        return records

    
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
        schema_encoding = '0' * self.table.num_columns
        for i, value in enumerate(columns):
            if value != None:
                schema_encoding[i] = 1
        rid = self.table.num_records
        self.table.num_records += 1
        time = datetime.now().strftime("%Y%m%d%H%M%S")
        indirection = rid
        
        
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
        selected_rows = []

        for key in self.table.key: #traverse through all keys in the table
            if start_range <= key <= end_range:
                query_column = [0] * self.table.num_columns
                query_column[aggregate_column_index] = 1
                selected_rows.append(self.select(key, 0, query_column))

        total_sum = 0
        for row in selected_rows:
            value = row.columns[aggregate_column_index]
        total_sum += value

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
