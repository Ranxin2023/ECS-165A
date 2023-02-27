"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

from lstore.page import Page
from lstore.btree import BTree
class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [list() for i in range(table.total_num_columns)]
        self.column_num = dict()
        self.table = table
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        return_list = []
        column_values = sorted(self.indices[column])
        for column_rid in column_values:
            if column_rid[0] == value:
                return_list.append(column_rid[1])
        return return_list

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        return_list = []
        column_values = sorted(self.indices[column])

        for column_rid in column_values:
            if column_rid[0] >= begin and column_rid[0] <=end:
                return_list.append(column_rid[1])
        return return_list

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass

    def push_index(self, columns):
        for i in range(1, self.table.total_num_columns):
            self.indices[i].append((columns[i], columns[0]))
            self.column_num[columns[i]] = i
    '''
    def lower_bound(self, list):
        low, up=0, len(list)-1
        while low<up:
            mid=(low+up)//2
    '''