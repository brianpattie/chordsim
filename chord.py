from hashlib import blake2b
import threading
import queue
import struct
import time
import random
import math

TIMEOUT = 2
HASH_BITS = 8

running = True
random.seed(time.time())

# Returns a the unsigned integer representation of 1 byte hash of the input string
# Used BLAKE2 because it allows you to specify the size of the hash
def hash(filename):
    h = blake2b(digest_size=1)
    h.update(filename.encode())
    return struct.unpack('B', h.digest())[0]

# Returns true if i is between lower bound lb and upper bound ub
def mod_between(i, lb, ub):
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
        self.orig_sender_id = sender_id
        self.sender_id      = sender_id
        self.receiver_id    = receiver_id
        self.filename       = filename
        self.data           = data

    def copy(self):
        return Message(self.type, self.sender_id, self.receiver_id, self.filename, self.data)

    def print(self):
        print('type:')
        print(self.type)
        print('orig_sender_id:')
        print(self.orig_sender_id)
        print('sender_id:')
        print(self.sender_id)
        print('receiver_id:')
        print(self.receiver_id)
        print('filename')
        print(self.filename)
        print('data:')
        print(self.data)

# Struct for use in each node's finger table.
# The finger table is used to find out which node has the requested resource in the DHT
class Finger():

    def __init__(self, start, node):
        self.start = start  # Start is currently not being used but will become important when nodes leave/join
        self.node = node    # Node identifies a ChordNodeself

# ChordNode threads represent nodes in the DHT
class ChordNode(threading.Thread):

    def __init__(self, id, predecessor, finger_table, queues):
        if id != None:
            threading.Thread.__init__(self)
            self.id = id                        # ID both uniquely identifies this node and specifies the last key in its portion of the identifier circle
            self.predecessor = predecessor      # The ID of the previous node in the identifier space
            self.finger_table = finger_table    # A table of finger objects that point to other nodes in the identifier space
            self.queues = queues                # Queues for messaging other nodes.  Stored in a dictionary.
            self.hash_table = {}                # The hash table part of the distributed hash table
        else:
            threading.Thread.__init__(self)
            self.id = None
            self.predecessor = None
            self.finger_table = []
            self.queues = queues
            self.hash_table = {}

    def join_network(self):

        self.id = random.randint(0, 255) # Generate random ID
        while self.id in self.queues.keys():
            self.id = random.randint(0, 255) # Don't repeat
        self.queues[self.id] = queue.Queue() # Add a queue to reach this node at

        # print('ID is ' + str(self.id));

        # Select a random node to message first to begin the join process
        queue_keys = list(self.queues.keys())
        queue_keys.remove('root')
        queue_keys.remove(self.id)

        # If no nodes are in the network, then initialize yourself as the first node
        if len(queue_keys) == 0:
            self.predecessor = self.id
            i = 0
            while i < HASH_BITS:
                start = (self.id + 2**i) % 2**HASH_BITS
                self.finger_table.append(Finger(start, self.id))
                i += 1
            return

        # Ask the random node for this node's predecessor
        rand_queue = queue_keys[random.randint(0, len(queue_keys)) - 1]
        self.queues[rand_queue].put(Message('FIND_PRED', self.id, rand_queue, None, self.id))
        msg = self.queues[self.id].get()
        self.predecessor = msg.data
        # print('Predecessor is ' + str(self.predecessor));

        # Ask this node's predecessor for its successor
        self.queues[self.predecessor].put(Message('FIND_SUCC', self.id, self.predecessor, None, self.id))
        msg = self.queues[self.id].get()
        finger = Finger(self.id + 1, msg.data)
        self.finger_table.append(finger)
        # print('Successor is ' + str(self.finger_table[0].node));

        # Ask your successor for the rest of your fingers
        # print('At Step: Get Finger Table')
        i = 1
        while i < HASH_BITS:
            start = (self.id + 2**i) % 2**HASH_BITS
            # As successor to find the predecessor of the finger node
            self.queues[self.finger_table[0].node].put(Message('FIND_PRED', self.id, self.finger_table[0].node, None, start))
            msg = self.queues[self.id].get()
            # The ask the predecessor of the finger node for it's successor
            self.queues[msg.data].put(Message('FIND_SUCC', self.id, self.finger_table[0].node, None, start))
            msg = self.queues[self.id].get()
            Finger(start, msg.data)
            self.finger_table.append(Finger(start, msg.data))
            i += 1

        # for f in range(0, len(self.finger_table)):
        #     print('Finger ' + str(f) + ' starts at ' + str(self.finger_table[f].start) + ' and points at Node ' + str(self.finger_table[f].node))

        # Update neighbours successors and predecessor
        self.stabalize()
        self.fix_all_fingers()

        # Notify other nodes to update finger tables

    def run(self):

        # Initialize Self
        if self.id == None:
            self.join_network();

        # print(str(self.id) + ' JOIN COMPLETE')
        self.report()

        while running:

            # if self.id != self.finger_table[0].node:
            #     self.stabalize()

            try:
                msg = self.queues[self.id].get(timeout = TIMEOUT)
            except:
                continue

            # print('Node ' + str(self.id) + ' received message type ' + str(msg.type) + ' from ' + str(msg.sender_id))
            # msg.print()
            if msg.type == 'GET':
                self.get(msg)

            elif msg.type == 'SET':
                # print('Node ' + str(self.id) + ' received set for ' + str(msg.filename))
                self.set(msg)

            elif msg.type == 'FIND_PRED':
                self.remote_find_predecessor(msg)

            # elif msg.type == 'TEST_FIND_PRED': # TESTING CODE, REMOVE
            #     # print(str(self.id) + ' received TEST_FIND_PRED message')
            #     p = self.find_predecessor(msg.data)
            #     print(str(p) + ' is the predecessor of ' + str(msg.data))

            elif msg.type == 'FIND_SUCC':
                self.remote_find_successor(msg)

            # elif msg.type == 'TEST_FIND_SUCC': # TESTING CODE, REMOVE
            #     # print(str(self.id) + ' received TEST_FIND_SUCC message')
            #     p = self.find_successor(msg.data)
            #     print(str(p) + ' is the successor of ' + str(msg.data))

            elif msg.type == 'STABALIZE':
                self.remote_stabalize(msg)

            elif msg.type == 'NOTIFY':
                self.remote_notify(msg)

            elif msg.type == 'REPORT':
                self.report()

            elif msg.type == 'SUCC_STABALIZE':
                self.succ_stabalize(msg)

            elif msg.type == 'PRED_STABALIZE':
                self.pred_stabalize(msg)

            elif msg.type == 'SET_SUCCESSOR':
                self.set_successor(msg)

            elif msg.type == 'SET_PREDECESSOR':
                self.set_predecessor(msg)

        print('Node ' + str(self.id) + ' exiting')
        exit()


    # Retreives a file from the DHT (which means printing it to screen for now)
    def get(self, msg):

        id = hash(msg.filename)
        if mod_between(id, self.predecessor + 1, self.id):
            self.queues[msg.orig_sender_id].put(Message('GET_REPLY', self.id, msg.orig_sender_id, msg.filename, self.hash_table[id]))
            print('Node ' + str(self.id) + ' performed GET on ' + str(msg.filename) + ' (Hashes to ' + str(id) + ') retrieving value: ' + str(self.hash_table[id]))
        else:
            s = self.find_successor(id)
            fwd_msg = msg.copy()
            fwd_msg.sender_id = self.id
            self.queues[s].put(fwd_msg)


    # Writes data to the DHT under hash(filename)
    def set(self, msg):

        id = hash(msg.filename)
        if mod_between(id, self.predecessor + 1, self.id):
            self.hash_table[id] = msg.data
            self.queues[msg.orig_sender_id].put(Message('SET_REPLY', self.id, msg.orig_sender_id, msg.filename, self.hash_table[id]))
            print('Node ' + str(self.id) + ' performed SET on ' + str(msg.filename) + ' (Hashes to ' + str(id) + ') setting value: ' + str(self.hash_table[id]))
        else:
            s = self.find_successor(id)
            fwd_msg = msg.copy()
            fwd_msg.sender_id = self.id
            self.queues[s].put(fwd_msg)


    # Find the successor by asking the predecessor who their successor is
    def find_successor(self, id):
        if mod_between(id, self.predecessor + 1, self.id):
            return self.id
        n = self.find_predecessor(id)
        if n == self.id:
            return self.finger_table[0].node
        self.queues[n].put(Message('FIND_SUCC', self.id, n, None, id))
        msg = self.queues[self.id].get()
        return msg.data


    def remote_find_successor(self, msg):
        self.queues[msg.orig_sender_id].put(Message('FIND_SUCC_REPLY', self.id, msg.orig_sender_id, None, self.finger_table[0].node))


    def find_predecessor(self, id):
        # If this node is the predecessor
        if mod_between(id, self.id + 1, self.finger_table[0].node):
            return self.id
        # Else some other node is the predecessor, forward the request to closest preceeding finger
        n = self.closest_preceding_finger(id)
        self.queues[n].put(Message('FIND_PRED', self.id, n, None, id))
        # Wait for reply
        msg = self.queues[self.id].get()
        return msg.data


    def remote_find_predecessor(self, msg):
        if mod_between(msg.data, self.id + 1, self.finger_table[0].node):
            self.queues[msg.orig_sender_id].put(Message('FIND_PRED_REPLY', self.id, msg.orig_sender_id, None, self.id))
        # Some other node is the succcessor, pass request to closest preceeding node
        else:
            n = self.closest_preceding_finger(msg.data)
            fwd_msg = msg.copy()
            fwd_msg.sender = self.id
            self.queues[n].put(fwd_msg)


    # Identifies the node on the fingering table that is closest to the target point on the identifier circle without going past it.
    # The finger table is sorted from closest to furthest.  Each entry points to a node that is twice as far as the previous entry.
    # The first entry points to this node's successor, and the last node points directly across the center of the identifier circle.
    def closest_preceding_finger(self, id):
        for f in reversed(self.finger_table):
            if mod_between(f.node, self.id + 1, id):
                return f.node
        # print('I am the closest preceeding finger')
        return self.id


    def stabalize(self):
        if self.id != self.finger_table[0].node:
            self.queues[self.finger_table[0].node].put(Message('SUCC_STABALIZE', self.id, self.finger_table[0].node, None, self.id))
        if self.id != self.predecessor:
            self.queues[self.predecessor].put(Message('PRED_STABALIZE', self.id, self.predecessor, None, self.id))

    def succ_stabalize(self, msg):
        if self.id == self.predecessor or mod_between(msg.data, self.predecessor, self.id):
            self.predecessor = msg.data
        else:
            self.queues[msg.orig_sender_id].put(Message('SET_SUCCESSOR', self.id, msg.orig_sender_id, None, self.predecessor))

    def set_successor(self, msg):
        self.finger_table[0].node = msg.data

    def pred_stabalize(self, msg):
        if self.id == self.finger_table[0].node or mod_between(msg.data, self.id, self.finger_table[0].node):
            self.finger_table[0].node = msg.data
        else:
            self.queues[msg.orig_sender_id].put(Message('SET_PREDECESSOR', self.id, msg.orig_sender_id, None, self.finger_table[0].node))

    def set_predecessor(self, msg):
        self.predecessor = msg.data


    def report(self):
        print('Node ID: ' + str(self.id))
        print('Predecessor: ' + str(self.predecessor))
        for f in range(0, len(self.finger_table)):
            print('Finger ' + str(f) + ': starts at ' + str(self.finger_table[f].start) + ' and points at Node ' + str(self.finger_table[f].node))

    def fix_all_fingers(self):
        print('Node ' + str(self.id) + ' fixing fingers')
        for i in range(1, HASH_BITS):
            self.finger_table[i].node = self.find_successor(self.finger_table[i].start)

    # # Sends message to successor, predecessor, and all nodes in finger table
    # def broadcast(self, message):
    #     for q_key in self.queues.keys():
    #         if q_key != self.id:
    #             self.queues[q_key].put(message)

# Main
# Construct queues dictionary
# TODO: CONSIDER WRAPPING THIS IN READWRITE LOCK
# Only written to when inserting a node, plan to only simulate one insertion at a time.
queues = {}
# queues[0] = queue.Queue()
# queues[42] = queue.Queue()
# queues[100] = queue.Queue()
# queues[172] = queue.Queue()
queues['root'] = queue.Queue()

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

# nodes = []
# nodes.append(ChordNode(0, 172, f0, queues))
# nodes.append(ChordNode(42, 0, f42, queues))
# nodes.append(ChordNode(100, 42, f100, queues))
# nodes.append(ChordNode(172, 100, f172, queues))

# Print hashes of test cases for reference
# print('Banana: ' + str(hash('Banana')))
# print('Turnip: ' + str(hash('Turnip')))
# print('Chinchilla: ' + str(hash('Chinchilla')))
# print('Komquat: ' + str(hash('Komquat')))
# print('Radish: ' + str(hash('Radish')))
# print('Bombomb: ' + str(hash('Bombomb')))

# Start ChordNodes
# for n in nodes:
#     n.start()

# Testing Code
nodes = []

for i in range(0, 3):
    nodes.append(ChordNode(None, None, None, queues))

print('STARTING NODES')

for n in nodes:
    n.start()
    time.sleep(2)

print('REPORTING STATE 1')

for n in nodes:
    n.report()
    time.sleep(2)


print('FIXING FINGERS')

for n in nodes:
    n.fix_all_fingers()
    time.sleep(2)
#
print('REPORTING STATE 3')

for n in nodes:
    n.report()
    time.sleep(2)

running = False

# queues[0].put(Message('SET', 'root', 0, 'Chinchilla', 1))
# time.sleep(1)
# queues[0].put(Message('SET', 'root', 42, 'Banana', 2))
# time.sleep(1)
# queues[0].put(Message('SET', 'root', 100, 'Bombomb', 3))
# time.sleep(1)
# queues[0].put(Message('SET', 'root', 172, 'Komquat', 4))
# time.sleep(1)
#
# queues[0].put(Message('GET', 'root', 0, 'Chinchilla', None))
# time.sleep(1)
# queues[0].put(Message('GET', 'root', 0, 'Banana', None))
# time.sleep(1)
# queues[0].put(Message('GET', 'root', 0, 'Bombomb', None))
# time.sleep(1)
# queues[0].put(Message('GET', 'root', 0, 'Komquat', None))
# time.sleep(1)
#
# queues[172].put(Message('SET', 'root', 0, 'Chinchilla', 100))
# time.sleep(1)
# queues[172].put(Message('SET', 'root', 42, 'Banana', 200))
# time.sleep(1)
# queues[172].put(Message('SET', 'root', 100, 'Bombomb', 300))
# time.sleep(1)
# queues[172].put(Message('SET', 'root', 172, 'Komquat', 400))
# time.sleep(1)
#
# queues[172].put(Message('GET', 'root', 0, 'Chinchilla', None))
# time.sleep(1)
# queues[172].put(Message('GET', 'root', 0, 'Banana', None))
# time.sleep(1)
# queues[172].put(Message('GET', 'root', 0, 'Bombomb', None))
# time.sleep(1)
# queues[172].put(Message('GET', 'root', 0, 'Komquat', None))
# time.sleep(1)
