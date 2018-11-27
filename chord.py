from hashlib import blake2b
import threading
import queue
import struct
import time

# Returns a 1 byte hash of the input string
# Used BLAKE2 because it allows you to specify the size of the hash
def hash(filename):
    h = blake2b(digest_size=1)
    h.update(filename.encode())
    return h.digest()

# Converts hash values into integers that can be used in direct comparisons
def bytes_to_uint8(bytes):
    return struct.unpack('B', bytes)[0]

# Returns true if i is between lower bound lb and upper bound ub
def mod_between_int(i, lb, ub):
    # TODO replace this with a slick mathmatical solution
    if lb < ub and i >= lb and i <= ub:
        return True
    if lb > ub and (i >= lb or i <= ub):
        return True
    return False

# Struct for messages
class Message():

    def __init__(self, type, sender_id, receiver_id, filename, data):
        self.type           = type
        self.sender_id      = sender_id
        self.receiver_id    = receiver_id
        self.filename       = filename
        self.data           = data

# Struct for use in each node's finger table.
# The finger table is used to find out which node has the requested resource in the DHT
class Finger():

    def __init__(self, start, node):
        self.start = start  # Start is currently not being used but will become important when nodes leave/join
        self.node = node    # Node identifies a ChordNodeself

# ChordNode threads represent nodes in the DHT
class ChordNode(threading.Thread):

    def __init__(self, id, successor, predecessor, finger_table, queues):
        threading.Thread.__init__(self)
        self.id = id                        # ID both uniquely identifies this node and specifies the last key in its portion of the identifier circle
        self.successor = successor          # The ID of the next node in the identifier space
        self.predecessor = predecessor      # The ID of the previous node in the identifier space
        self.finger_table = finger_table    # A table of finger objects that point to other nodes in the identifier space
        self.queues = queues                # Queues for messaging other nodes.  Stored in a dictionary.
        self.hash_table = {}                # The hash table part of the distributed hash table

    def run(self):
        while True:
            msg = self.queues[self.id].get()
            if msg.type == 'GET':
                self.get(msg.filename)
            elif msg.type == 'SET':
                self.set(msg.filename, msg.data)

    # Retreives a file from the DHT (which means printing it to screen for now)
    def get(self, filename):
        id = hash(filename)
        # If I have the ID
        if mod_between_int(bytes_to_uint8(id), self.predecessor+1, self.id):

            # Print if the value is there, else notify that it is not
            if id in self.hash_table.keys():
                print('Node: ' + str(self.id) + ' \tgetting value at key(' + str(id) + '): ' + str(self.hash_table[id]))
            else:
                print('Node: ' + str(self.id) + ' \thas no value at key(' + str(id) + ')')

        # If my successor has it
        elif mod_between_int(bytes_to_uint8(id), self.id+1, self.successor):
            self.remote_get(self.successor, filename)

        # Else some other node has it
        else:
            self.remote_get(self.closest_preceding_finger(id), filename)

    # Sends a Get message a node selected from the fingering table
    def remote_get(self, node, filename):
        # print('Node: ' + str(self.id) + ' forwarding get(' + filename + ') to Node: ' + str(node))
        self.queues[node].put(Message('GET', self.id, node, filename, None))

    # Writes data to the DHT under hash(filename)
    def set(self, filename, data):
        id = hash(filename)
        # If I have the ID
        if mod_between_int(bytes_to_uint8(id), self.predecessor+1, self.id):
            # print('Node: ' + str(self.id) + ' \tsetting value at key(' + str(id) + ') to: ' + str(data))
            self.hash_table[id] = data

        # If my successor has it
        elif mod_between_int(bytes_to_uint8(id), self.id+1, self.successor):
            self.remote_set(self.successor, filename, data)

        # Else some other node has it
        else:
            self.remote_set(self.closest_preceding_finger(id), filename, data)

    # Sends a Set message to a node selected from the fingering table
    def remote_set(self, node, filename, data):
        # print('Node: ' + str(self.id) + ' forwarding set(' + filename + ') to Node: ' + str(node))
        self.queues[node].put(Message('SET', self.id, node, filename, data))

    # Identifies the node on the fingering table that is closest to the target point on the identifier circle without going past it.
    # The finger table is sorted from closest to furthest.  Each entry points to a node that is twice as far as the previous entry.
    # The first entry points to this node's successor, and the last node points directly across the center of the identifier circle.
    def closest_preceding_finger(self, id):
        for f in reversed(self.finger_table):
            if mod_between_int(f.node, self.id, bytes_to_uint8(id)):
                return f.node
        return self.id # This should never be reached.  If it is reached, then I'm having a bad day.


# Main
# Construct queues dictionary
queues = {}
queues[0] = queue.Queue()
queues[42] = queue.Queue()
queues[100] = queue.Queue()
queues[172] = queue.Queue()

# 0's finger table
f0 = []
f0.append(Finger(1, 42))
f0.append(Finger(2, 42))
f0.append(Finger(4, 42))
f0.append(Finger(8, 42))
f0.append(Finger(16, 42))
f0.append(Finger(32, 42))
f0.append(Finger(64, 100))
f0.append(Finger(128, 172))

# 42's finger table
f42 = []
f42.append(Finger(43, 100))
f42.append(Finger(44, 100))
f42.append(Finger(46, 100))
f42.append(Finger(50, 100))
f42.append(Finger(58, 100))
f42.append(Finger(74, 100))
f42.append(Finger(106, 172))
f42.append(Finger(170, 172))

# 100's finger table
f100 = []
f100.append(Finger(101, 172))
f100.append(Finger(102, 172))
f100.append(Finger(104, 172))
f100.append(Finger(108, 172))
f100.append(Finger(116, 172))
f100.append(Finger(132, 172))
f100.append(Finger(164, 172))
f100.append(Finger(228, 0))

# 172's finger table
f172 = []
f172.append(Finger(173, 0))
f172.append(Finger(174, 0))
f172.append(Finger(176, 0))
f172.append(Finger(180, 0))
f172.append(Finger(188, 0))
f172.append(Finger(204, 0))
f172.append(Finger(236, 0))
f172.append(Finger(44, 100))

nodes = []
nodes.append(ChordNode(0, 42, 172, f0, queues))
nodes.append(ChordNode(42, 100, 0, f42, queues))
nodes.append(ChordNode(100, 172, 42, f100, queues))
nodes.append(ChordNode(172, 0, 100, f172, queues))

# Print hashes of test cases for reference
print('Banana: ' + str(hash('Banana')))
print('Turnip: ' + str(hash('Turnip')))
print('Chinchilla: ' + str(hash('Chinchilla')))
print('Komquat: ' + str(hash('Komquat')))

# Start ChordNodes
for n in nodes:
    n.start()

# Testing Code
queues[0].put(Message('SET', None, None, 'Banana', 0))
queues[42].put(Message('SET', None, None, 'Turnip', 1))
queues[100].put(Message('SET', None, None, 'Chinchilla', 2))
queues[172].put(Message('SET', None, None, 'Komquat', 3))

time.sleep(2)

queues[0].put(Message('GET', None, None, 'Banana', None))
queues[0].put(Message('GET', None, None, 'Turnip', None))
queues[0].put(Message('GET', None, None, 'Chinchilla', None))
queues[0].put(Message('GET', None, None, 'Komquat', None))


# Mod_Between Test
# bytes = hash('banana')
# print(bytes)
# print(mod_between(bytes, 120, 122))


# Mod_between Test
# print(mod_between_int(1, 4, 8)) # False
# print(mod_between_int(2, 4, 8)) # False
# print(mod_between_int(3, 4, 8)) # False
# print(mod_between_int(4, 4, 8)) # True
# print(mod_between_int(5, 4, 8)) # True
# print(mod_between_int(6, 4, 8)) # True
# print(mod_between_int(7, 4, 8)) # True
# print(mod_between_int(8, 4, 8)) # True
# print(mod_between_int(9, 4, 8)) # False
# print(mod_between_int(10, 4, 8)) # False
# print(mod_between_int(188, 173, 0)) # True
