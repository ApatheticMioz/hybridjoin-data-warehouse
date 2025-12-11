# Doubly linked list for HYBRIDJOIN queue

class Node:
    def __init__(self, key):
        self.key = key
        self.prev = None
        self.next = None

class DoublyLinkedList:
    
    def __init__(self):
        self.head = None
        self.tail = None
        self.size = 0
    
    def append(self, key):
        new_node = Node(key)
        if self.tail is None:
            self.head = self.tail = new_node
        else:
            self.tail.next = new_node
            new_node.prev = self.tail
            self.tail = new_node
        self.size += 1
        return new_node
    
    def remove(self, node):
        if node is None:
            return
        
        if node.prev:
            node.prev.next = node.next
        else:
            self.head = node.next
        
        if node.next:
            node.next.prev = node.prev
        else:
            self.tail = node.prev
        
        self.size -= 1
    
    def peek_front(self):
        return self.head.key if self.head else None
    
    def is_empty(self):
        return self.size == 0
    
    def __len__(self):
        return self.size
