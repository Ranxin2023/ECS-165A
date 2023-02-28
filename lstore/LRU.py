from lstore.config import *
'''
class LRUCache {
private:
    int max_capacity;
    int capacity=0;
    DoubleLinkedList * head;
    DoubleLinkedList * tail;
    map<int, DoubleLinkedList *>double_list_map;




public:
    LRUCache(int capacity) {
        max_capacity=capacity;
        head=new DoubleLinkedList();
        tail=new DoubleLinkedList();
        head->next=tail;
        tail->prev=head;
    }




};

/**
 * Your LRUCache object will be instantiated and called as such:
 * LRUCache* obj = new LRUCache(capacity);
 * int param_1 = obj->get(key);
 * obj->put(key,value);
 */
'''
'''
class DoubleLinkedList{
public:
    int key;
    int value;
    DoubleLinkedList * prev=nullptr;
    DoubleLinkedList * next=nullptr;
    DoubleLinkedList(){

    }
    DoubleLinkedList(int key, int value){
        this->key=key;
        this->value=value;
    }

};
'''
class DoubleLinkedList:
    def __init__(self, key, value):
        self.key=key
        self.value=value
        self.next=None
        self.prev=None

class LRU:
    def __init__(self):
        self.capacity=0
        self.head=DoubleLinkedList()
        self.tail=DoubleLinkedList()
        self.head.next=self.tail
        self.tail.prev=self.head
        self.double_list_map = {}
    '''
    void delete_node(DoubleLinkedList * node){
        //if(node!=nullptr)cout<<node->key<<endl;
        node->next->prev=node->prev;
        node->prev->next=node->next;
    }
    '''
    def delete_node(self, node: DoubleLinkedList):
        node.next.prev = node.prev
        node.prev.next = node.next
    '''
    void add_node(int key, int value){
            DoubleLinkedList * prev_next=head->next;
            head->next=new DoubleLinkedList(key, value);
            prev_next->prev=head->next;
            head->next->prev=head;
            head->next->next=prev_next;
            double_list_map[key]=head->next;
        }
    '''
    def add_node(self, key, value):
        prev_next = self.head.next
        self.head.next = DoubleLinkedList(key, value)
        prev_next.prev = self.head.next
        self.head.next.prev = self.head
        self.head.next.next = prev_next
        self.double_list_map[key] = self.head.next

    '''
    void move_to_head(DoubleLinkedList * node){
        //DoubleLinkedList * node=double_list_map[key];
        delete_node(node);
        add_node(node->key, node->value);
    }
    '''

    def move_to_head(self, node: DoubleLinkedList):
        self.delete_node(node)
        self.add_node(node.key, node.value)
    '''
    int delete_tail(){
        DoubleLinkedList * pre_node=tail->prev;
        delete_node(pre_node);
        return pre_node->key;
    }
    '''
    def delete_tail(self):
        pre_node = self.tail.prev
        self.delete_node(pre_node)
        return pre_node.key

    #public functions
    '''
    int get(int key) {
        if(double_list_map.count(key)){
            move_to_head(double_list_map[key]);
            return double_list_map[key]->value;
        }

        return -1;

    }
    '''

    def get(self, key):
        if key in self.double_list_map:
            self.move_to_head(self.double_list_map[key])
            return self.double_list_map[key].value
        return None

    '''
    void put(int key, int value) {
        if(double_list_map.count(key)){
            double_list_map[key]->value=value;
            move_to_head(double_list_map[key]);
        }
        else{
            add_node(key, value);
            double_list_map[key]=head->next;
            capacity++;
            if(capacity>max_capacity){
                int old_key=delete_tail();
                double_list_map.erase(old_key);
                capacity--;
            }
        }
    }
    '''
    def put(self, key, value):
        if key in self.double_list_map:
            self.double_list_map[key].value = value
            self.move_to_head(self.double_list_map[key])
        else:
            self.add_node(key, value)
            self.double_list_map[key] = self.head.next
            self.capacity += 1
            if self.capacity>BUFFER_POOL_SIZE:
                old_key=self.delete_tail()
                self.double_list_map.pop(old_key)
                self.capacity -= 1
