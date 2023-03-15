from lstore.index import Index
import threading
# from readerwriterlock import rwlock
from collections import defaultdict

class LockManage:
    
    def __init__(self):
        self.locks = defaultdict(RWLock())
       
    def acquire_reader(self,rid):
        return self.locks[rid].acquire_rlock()

    def release_reader(self,rid):
        return self.locks[rid].release_rlock()
    
    def acquire_writer(self,rid):
        return self.locks[rid].acquire_wlock()
    
    def release_writer(self,rid):
        return self.locks[rid].release_wlock() 
        
        
        
class RWLock:
    
    def __init__(self):
        self.lock = threading.Lock()
        self.reader = 0
        self.writer = False 
        
    def acquire_rlock(self):         # get read lock
        self.lock.acquire()

        if self.writer:        # can't read if writer
            self.lock.release()
            return False
        else:
            self.reader += 1
            self.lock.release()
            return True

    def release_rlock(self):
        self.lock.acquire()
        self.reader -= 1
        self.lock.release()

    def acquire_wlock(self):
        self.lock.acquire()

        if self.reader != 0:        # if something is reader, can't write
            self.lock.release()
            return False
        elif self.writer:      # if something else is writer, can't write
            self.lock.release()
            return False
        else:
            self.writer = True
            self.lock.release()
            return True

    def release_wlock(self):
        self.lock.acquire()
        self.writer = False
        self.lock.release()
