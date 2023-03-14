"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

from lstore.page import Page
from BTrees.OOBTree import OOBTree

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None for _ in range(table.num_columns)]
        self.key_column = table.key_column
        self.column_num = dict()
        self.table = table
        
        pass

    def has_index(self, column):
        if self.indices[column] == None:
            return False
        else:
            return True
        
    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        #return_list = []
        column_btree = self.indices[column]
        #print(type(column_btree))
        if not column_btree.has_key(value):
            return []
        return column_btree[value]

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        return_list = []
        column_btree = self.indices[column]
        for list1 in list(column_btree.values(min=begin, max=end)):
            return_list += list1
        return return_list


    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if self.indices[column_number] == None:
            self.indices[column_number] = OOBTree()


    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        #del self.indices[column_number]
        self.indices[column_number] = None


    def push_index(self, columns, rid):
        for i in range(1, self.table.num_columns):
            if self.indices[i] == None:
                self.create_index(i)
            #print("indices:{}".format(self.indices))
            if not self.indices[i].has_key(columns[i]):
                self.indices[i][columns[i]]= [rid]
            else:
                self.indices[i][columns[i]].append(rid)
            self.column_num[columns[i]] = i

    '''
    def lower_bound(self, list):
        low, up=0, len(list)-1
        while low<up:
            mid=(low+up)//2
    '''