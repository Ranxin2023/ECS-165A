from lstore.table import Table, Record
from lstore.lock_manage import RWLock
from lstore.query import Query
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.table = None
        self.read_locks = set()
        self.write_locks = set()
        self.insert_locks = set()
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        if self.table == None:
            self.table = table
        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            key = args[0]
            if key not in self.table.lock_manager:
                self.insert_locks.add(key)
                self.table.lock_manager[key] = RWLock()
            if key not in self.write_locks and key not in self.insert_locks:
                if self.table.lock_manager[key].acquire_wlock():
                    self.write_locks.add(key)
                else:
                    return self.abort()
            
        # for query, args in self.queries:
        #     result = query(*args)
        #     # If the query has failed the transaction should abort
        #     if result == False:
        #         return self.abort()
        return self.commit()

    
    def abort(self):
        #TODO: do roll-back and any other necessary operations
        for key in self.read_locks:
            self.table.lock_manager[key].release_rlock()
        for key in self.write_locks:
            self.table.lock_manager[key].release_writer()
        for key in self.insert_locks:
            del self.table.lock_manager[key]
        return False
        

    
    def commit(self):
        # TODO: commit to database
        
        # for query, args in self.queries:
        #     query(*args)
        #     # remove lock from lock manager after deleting record
        #     if query == Query.delete:
        #         del self.table.lock_manager[key]
        #         if key in self.write_locks:
        #             self.insert_locks.remove(key)
        #         if key in self.insert_locks:
        #             self.insert_locks.remove(key)
        
        for key in self.read_locks:
            self.table.lock_manager[key].release_rlock()
        for key in self.write_locks:
            self.table.lock_manager[key].release_wlock()
        for key in self.insert_locks:
            self.table.lock_manager[key].release_wlock()
        return True
    
'''
    def abort(self):
        #TODO: do roll-back and any other necessary operations
        for key in self.read_locks:
            self.table.lock_manager[key].release_read()
        for key in self.write_locks:
            self.table.lock_manager[key].release_write()
        for key in self.insert_locks:
            del self.table.lock_manager[key]
        return False

    def commit(self):
        # TODO: commit to database
        for query, args in self.queries:
            query(*args)
            # remove lock from lock manager after deleting record
            if query == Query.delete:
                del self.table.lock_manager[key]
                if key in self.write_locks:
                    self.insert_locks.remove(key)
                if key in self.insert_locks:
                    self.insert_locks.remove(key)
        for key in self.read_locks:
            self.table.lock_manager[key].release_read()
        for key in self.write_locks:
            self.table.lock_manager[key].release_write()
        for key in self.insert_locks:
            self.table.lock_manager[key].release_write()
        return True
'''
