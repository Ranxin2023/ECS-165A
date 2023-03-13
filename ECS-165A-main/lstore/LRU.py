from lstore.config import *

class Node:
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.next = None
        self.prev = None

class LRU:
    def __init__(self, capacity = 2000):
        self.capacity = capacity
        self.num = 0
        self.head = None
        self.tail = None
        # self.head.next=self.tail
        # self.tail.prev=self.head
        self.double_list_map = {}
    
    def delete_node(self, node: Node):
        # delete node is head
        if node.prev == None:
            self.head = node.next
            self.head.prev = None
        # delete node is tail
        elif node.next == None:
            self.tail = node.prev
            self.tail.next = None
        else:
            node.next.prev = node.prev
            node.prev.next = node.next
        del self.double_list_map[node.key]
    
    def add_node(self, key, value):
        newNode = Node(key, value)
        if self.num == 0:
            self.head = newNode
            self.tail = newNode   
            self.head.next = self.tail
            self.tail.prev = self.head
        elif self.num == 1:
            newNode.next = self.tail
            self.tail.prev = newNode
            self.head = newNode
        else:
            newNode.next = self.head
            self.head.prev = newNode
            self.head = newNode  
        
        self.num += 1
        self.double_list_map[key] = newNode

    def move_to_head(self, node: Node):
        self.delete_node(node)
        self.add_node(node.key, node.value)
    
    def delete_tail(self):
        pre_node = self.tail.prev
        self.delete_node(pre_node)
        return pre_node.key

    def get(self, key):
        if key in self.double_list_map:
            self.move_to_head(self.double_list_map[key])
            return self.double_list_map[key].value
        return None

    def put(self, key, value):
        if key in self.double_list_map:
            self.double_list_map[key].value = value
            self.move_to_head(self.double_list_map[key])
        else:
            self.add_node(key, value)
            if self.num > BUFFER_POOL_SIZE:
                old_key=self.delete_tail()
                del self.double_list_map[old_key]
                self.num -= 1
                
    def printLRU(self):
        for key in self.double_list_map:
            print(str(key) + " " + str(self.double_list_map[key].value))
            