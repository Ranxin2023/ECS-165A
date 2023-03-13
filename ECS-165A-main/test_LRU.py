from lstore.LRU import LRU

temp = LRU(10)
key = 9123456
value = 10

for i in range(5):  
    temp.put(key, value)
    key += 1
    value += 1

temp.printLRU()
print(str(temp.head.key) + ": " + str(temp.head.value))
temp.put(9123456, 100)
temp.printLRU()
print(str(temp.head.key) + ": " + str(temp.head.value))