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
    if lb < ub and i >= lb and i <= ub:
        return True
    if lb > ub and (i >= lb or i <= ub):
        return True
    return False

# Struct for messages NEW
# class Message():
#
#     def __init__(self, type, sender_id, file_id=None, file_data=None, node=None, finger_num=None):
#         self.type           = type
#         self.orig_sender_id = sender_id
#         self.sender_id      = sender_id
#         self.file_id        = file_id
#         self.file_data      = file_data
#         self.node           = node
#         self.finger_num     = finger_num
#
#     # Returns a new message with the same values
#     def copy(self):
#         return Message(self.type, self.sender_id, file_id = self.file_id, file_data = self.file_data, node = self.node, finger_num = self.finger_num)
#
#     def print(self):
#         print('type:')
#         print(self.type)
#         print('orig_sender_id:')
#         print(self.orig_sender_id)
#         print('sender_id:')
#         print(self.sender_id)
#         print('file_id')
#         print(self.file_id)
#         print('file_data:')
#         print(self.file_data)
#         print('node:')
#         print(self.node)
#         print('finger_num:')
#         print(self.finger_num)

# Struct for messages OLD
class Message():

    def __init__(self, type, sender_id, filename, data): #FIXME
        self.type           = type
        self.orig_sender_id = sender_id
        self.sender_id      = sender_id
        self.filename       = filename
        self.data           = data

    # Returns a new message with the same values
    def copy(self):
        return Message(self.type, self.sender_id, self.filename, self.data)#FIXME

    # Prints all fields of the message.  Used for debugging.
    def print(self):
        print('type:')
        print(self.type)
        print('orig_sender_id:')
        print(self.orig_sender_id)
        print('sender_id:')
        print(self.sender_id)
        print('filename')
        print(self.filename)
        print('data:')
        print(self.data)

# Struct for use in each node's finger table.
# The finger table is used to find out which node has the requested resource in the DHT
# It contains references an starting ID, and the ID of the nearest Node that follows it.
# If Nodes have been added since the last time this finger was updated, it may point to
# a different node.
class Finger():

    def __init__(self, start, node):
        self.start = start  # An ID
        self.node = node    # The ID of the nearest node with ID greater than 'start'

# ChordNode threads represent nodes in the Chord Distributed Hash table
class ChordNode(threading.Thread):

    def __init__(self, queues):

        threading.Thread.__init__(self)
        self.id = None                          # ID both uniquely identifies this node and specifies the last key in its portion of the identifier circle
        self.predecessor = None                 # The ID of the previous node in the identifier space
        self.finger_table = []                  # A table of finger objects that point to other nodes in the identifier space
        self.queues = queues                    # Queues for messaging other nodes.  Stored in a dictionary.
        self.msg_buf = []                       # Stores messages that arrive out of order
        self.hash_table = {}                    # The hash table part of the distributed hash table
        self.update_required = False

    def join_network(self):

        self.id = random.randint(0, 255)        # Generate random ID
        while self.id in self.queues.keys():    # Don't use an ID that is already in use
            self.id = random.randint(0, 255)    # Keep trying random ID's until you finds a free one
        self.queues[self.id] = queue.Queue()    # Add a queue for other node's to reach you at

        # print('ID is ' + str(self.id));

        queue_keys = list(self.queues.keys())   # queue_keys will be used to select a node to help initialzie you.
        queue_keys.remove(self.id)              # Don't use yourself.  You don't know anything.
        queue_keys.remove('root')               # Don't use the root message queue.  It won't be helpful.


        if len(queue_keys) == 0:                                    # If no nodes are in the network, then initialize yourself as the first node.
            self.predecessor = self.id                              # You are your own predecessor.
            i = 0
            while i < HASH_BITS:                                    # Set all your finger's to point at yourself.
                start = (self.id + 2**i) % 2**HASH_BITS             # There's nobody else to point at.
                self.finger_table.append(Finger(start, self.id))
                i += 1
            return

        rand_queue = queue_keys[random.randint(0, len(queue_keys)) - 1]             # Select a random node.
        self.queues[rand_queue].put(Message('FIND_PRED', self.id, None, self.id))   # Ask it to tell you your predecessor. #FIXME
        msg = self.wait_for_message_type('FIND_PRED_REPLY')
        self.predecessor = msg.data

        self.queues[self.predecessor].put(Message('FIND_SUCC', self.id, None, self.id))   # Ask your predecessor for your successor. #FIXME
        msg = self.wait_for_message_type('FIND_SUCC_REPLY')                               # Its current successor is probably your successor
        finger = Finger(self.id + 1, msg.data)
        self.finger_table.append(finger)                                                  # Store your seccessor in your first (and closest) finger.

        i = 1
        while i < HASH_BITS:                                # Ask your successor for all the rest of your fingers
            start = (self.id + 2**i) % 2**HASH_BITS         # <start> is an ID some distance away
            succ = self.find_successor(start)               # <succ> is the successor of <start>
            Finger(start, succ)
            self.finger_table.append(Finger(start, succ))
            i += 1

        self.stabalize()    # Your neigbours should point at you.  Tell them you exist.

        self.queues[self.finger_table[0].node].put(Message('DATA_REQUEST', self.id, None, None)) # Ask your successor for your share of it's partition #FIXME


    def run(self):

        self.join_network();    # Join the network by initializing your predecessor and finger values.  Also tell the neighbouring nodes to point at you.

        while running:          # Main loop

            if len(self.msg_buf) > 0:
                msg = self.msg_buf.pop(0)                               # Read any messages you buffered while you were waiting for a specific message.
            else:
                try:
                    msg = self.queues[self.id].get(timeout = TIMEOUT)   # Wait some time for a message.  This timeouts so that the node can perform stabalizations, but I didn't get to that.  Left it in.
                except:
                    continue

            # print('Node ' + str(self.id) + ' received message type ' + str(msg.type) + ' from ' + str(msg.sender_id))
            # msg.print()
            if msg.type == 'GET':                   # GET: If you have the value they are looking for, return it.  Otherwise pass it along.
                self.get(msg)

            elif msg.type == 'SET':                 # SET: If the key falls in your partition, set it to your local hash table.  Otherwise pass it along.
                self.set(msg)

            elif msg.type == 'FIND_PRED':           # FIND_PRED: A node is looking for a predecessor to some ID.  If it's you, reply with a message.  Otherwise pass it along.
                self.remote_find_predecessor(msg)

            elif msg.type == 'FIND_SUCC':           # FIND_SUCC: A node is looking for a successor to some ID.  If it's you, reply with a message.  Otherwise pass it along.
                self.remote_find_successor(msg)

            elif msg.type == 'SUCC_STABALIZE':      # SUCC_STABALIZE: A new node has joined the network and it thinks it's your predecessor.  Check the ID it sent you and update accordingly.
                self.succ_stabalize(msg)

            elif msg.type == 'PRED_STABALIZE':      # PRED_STABALIZE: A new node has joined the network and it thinks it's your successor.  Check the ID it sent you and update accordingly.
                self.pred_stabalize(msg)

            elif msg.type == 'NOTIFY':
                self.remote_notify(msg)

            elif msg.type == 'REPORT':
                self.report()

            elif msg.type == 'SET_SUCCESSOR':
                self.set_successor(msg)

            elif msg.type == 'SET_PREDECESSOR':
                self.set_predecessor(msg)

            elif msg.type == 'DATA_REQUEST':
                self.send_partition_data(msg)

            elif msg.type == 'DATA_TRANSFER':
                self.insert_data(msg)

            elif msg.type == 'FIX_FINGERS':
                self.fix_all_fingers()

            elif msg.type == 'LEAVE_NETWORK':
                self.leave_network()

            elif msg.type == 'PREDECESSOR_LEAVING':
                self.predecessor = msg.data

            elif msg.type == 'SUCCESSOR_LEAVING':
                self.finger_table[0].node = msg.data


            if self.update_required:
                self.fix_all_fingers()

            msg = None

        print('Node ' + str(self.id) + ' exiting')
        exit()


    # Retreives a file from the DHT (which means printing it to screen for now)
    def get(self, msg):

        id = hash(msg.filename)
        if mod_between(id, self.predecessor + 1, self.id):
            self.queues[msg.orig_sender_id].put(Message('GET_REPLY', self.id, msg.filename, self.hash_table[id])) #FIXME
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
            self.queues[msg.orig_sender_id].put(Message('SET_REPLY', self.id, msg.filename, self.hash_table[id])) #FIXME
            print('Node ' + str(self.id) + ' performed SET on ' + str(msg.filename) + ' (Hashes to ' + str(id) + ') setting value: ' + str(self.hash_table[id]))
        else:
            s = self.find_successor(id)
            fwd_msg = msg.copy()
            fwd_msg.sender_id = self.id
            self.queues[s].put(fwd_msg)


    # Find the successor by asking the predecessor who their successor is
    def find_successor(self, id):
        if mod_between(id, self.predecessor + 1, self.id):              # If this node is <ID>'s successor...
            return self.id                                              # Return this node's ID
        n = self.find_predecessor(id)                                   # Else some other node is the successor.  Start by finding the predecessor.
        if n == self.id:                                                # If this node is the predecessor, then this node's successor is <ID>'s successor
            return self.finger_table[0].node                            # Return this node's ID.
        self.queues[n].put(Message('FIND_SUCC', self.id, None, id))     # Else some other node is the predecessor.  Ask that node for it's successor    #FIXME
        msg = self.wait_for_message_type('FIND_SUCC_REPLY')             # Wait for the reply
        return msg.data                                                 # Return the ID in the message sent by <ID>'s successor.


    def remote_find_successor(self, msg):
        self.queues[msg.orig_sender_id].put(Message('FIND_SUCC_REPLY', self.id, None, self.finger_table[0].node)) #FIXME


    def find_predecessor(self, id):
        if mod_between(id, self.id + 1, self.finger_table[0].node):     # If this node is <ID>'s predecessor...
            return self.id                                              # return this node's ID
        n = self.closest_preceding_finger(id)                           # Else some other node is the predecessor, forward the request to closest preceeding finger
        self.queues[n].put(Message('FIND_PRED', self.id, None, id))     # Send the message          #FIXME
        msg = self.wait_for_message_type('FIND_PRED_REPLY')             # Wait for the reply
        return msg.data                                                 # Return the ID in the message sent by <ID>'s predecessor.


    def remote_find_predecessor(self, msg):
        # If this node is the predecessor
        if mod_between(msg.data, self.id + 1, self.finger_table[0].node):
            self.queues[msg.orig_sender_id].put(Message('FIND_PRED_REPLY', self.id, None, self.id))         #FIXME
        # Some other node is the successor, pass request to closest preceeding node
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
            if mod_between(f.node, self.id + 1, id) and self.node_exists(f.node):
                return f.node
        # print('I am the closest preceeding finger')
        return self.id


    def node_exists(self, id):
        if id in self.queues.keys():
            return True
        else:
            print('Node: ' + str(self.id) + ' could not send message to ' + str(id) + ' because there was no queue for it')
            self.update_required = True
            return False


    def stabalize(self):
        if self.id != self.finger_table[0].node:
            self.queues[self.finger_table[0].node].put(Message('SUCC_STABALIZE', self.id, None, self.id))      #FIXME
        if self.id != self.predecessor:
            self.queues[self.predecessor].put(Message('PRED_STABALIZE', self.id, None, self.id))                  #FIXME

    def succ_stabalize(self, msg):
        if self.id == self.predecessor or mod_between(msg.data, self.predecessor, self.id):
            self.predecessor = msg.data
        else:
            self.queues[msg.orig_sender_id].put(Message('SET_SUCCESSOR', self.id, None, self.predecessor))          #FIXME

    def set_successor(self, msg):
        self.finger_table[0].node = msg.data

    def pred_stabalize(self, msg):
        if self.id == self.finger_table[0].node or mod_between(msg.data, self.id, self.finger_table[0].node):
            self.finger_table[0].node = msg.data
        else:
            self.queues[msg.orig_sender_id].put(Message('SET_PREDECESSOR', self.id, None, self.finger_table[0].node))        #FIXME

    def set_predecessor(self, msg):
        self.predecessor = msg.data


    def report(self):
        print('------------------------------------------')
        print('Node ID: ' + str(self.id))
        print('Predecessor: ' + str(self.predecessor))
        for f in range(0, len(self.finger_table)):
            print('Finger ' + str(f) + ': starts at ' + str(self.finger_table[f].start) + ' and points at Node ' + str(self.finger_table[f].node))

        print('Local Hash Table Contents:')

        for id in list(self.hash_table.keys()):
            print(str(id) + '\t= ' + str(self.hash_table[id]))

    def fix_all_fingers(self):
        print('Node ' + str(self.id) + ' fixing fingers')
        for i in range(1, HASH_BITS):
            x = self.find_successor(self.finger_table[i].start)
            self.finger_table[i].node = x
        self.update_required = False

    # Send any data that should belong to your predecessor.  Called when a new predecessor requests it the data.
    def send_partition_data(self, msg):
        for id in list(self.hash_table.keys()):
            if not mod_between(id, self.predecessor + 1, self.id):
                # print('Node ' + str(self.id) + ' sending hash ' + str(id) + ' with data ' + str(self.hash_table[id]) + ' to Node ' + str(msg.orig_sender_id))
                self.queues[msg.orig_sender_id].put(Message('DATA_TRANSFER', self.id, id, self.hash_table[id]))           #FIXME
                self.hash_table.pop(id, None)

    # Send all data to your successor before you leave the network.  Called as part of leave_network()
    def relinquish_partition_data(self):
        for id in list(self.hash_table.keys()):
            print('Node ' + str(self.id) + ' giving away ' + str(id) + ' before I die.')
            # print('Node ' + str(self.id) + ' sending hash ' + str(id) + ' with data ' + str(self.hash_table[id]) + ' to Node ' + str(msg.orig_sender_id))
            self.queues[self.finger_table[0].node].put(Message('DATA_TRANSFER', self.id, id, self.hash_table[id]))         #FIXME
            self.hash_table.pop(id, None)

    def insert_data(self, msg):
        print('Node ' + str(self.id) + ' receiving ' + str(msg.filename) + ':' + str(msg.data))
        self.hash_table[msg.filename] = msg.data

    def wait_for_message_type(self, type):
        msg = self.queues[self.id].get()
        while msg.type != type:
            print('Node ' + str(self.id) + ' adding message:')
            # msg.print()
            self.msg_buf.append(msg)
        return msg

    def leave_network(self):
        # Pass all your info to your successor
        self.relinquish_partition_data()
        self.queues[self.finger_table[0].node].put(Message('PREDECESSOR_LEAVING', self.id, None, self.predecessor))        #FIXME
        self.queues[self.predecessor].put(Message('SUCCESSOR_LEAVING', self.id, None, self.predecessor))                 #FIXME
        del self.queues[self.id]
        print('Node ' + str(self.id) + ' leaving network and exiting')
        exit()
        # Delete your queue
        # Notify your predecessor and successor



# Main
# UNIVERSAL TEST SET UP
queues = {}
queues['root'] = queue.Queue()

# TEST 1: PARTITION PASSING DURING NODE JOIN---
# nodes = []
# nodes.append(ChordNode(queues))
# nodes[0].start()
#
# time.sleep(0.25)
#
# queues[nodes[0].id].put(Message('SET', 'root', 'Chinchilla', 'Chinchilla'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Artichoke', 'Artichoke'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Mozzerella', 'Mozzerella'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Spinnach', 'Spinnach'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Alfredo', 'Alfredo'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Komquat', 'Komquat'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Rosemary', 'Rosemary'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Shrimp', 'Shrimp'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Halibut', 'Halibut'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Corn', 'Corn'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Yams', 'Yams'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Horseraddish', 'Horseraddish'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Garlic', 'Garlic'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Cauliflower', 'Cauliflower'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Pasta', 'Pasta'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Mushroom', 'Mushroom'))
# time.sleep(0.25)
#
#
# # Add New Nodes now that there is data in the first node
# new_nodes = []
# for i in range(0,5):
#     new_nodes.append(ChordNode(queues))
#
# for n in new_nodes:
#     n.start()
#     time.sleep(0.25)
#
# print('FINAL VALUES')
#
# nodes[0].report()
# for n in new_nodes:
#     n.report()
#     time.sleep(0.25)

# queues[nodes[0].id].put(Message('GET', 'root', 'Chinchilla', 'Chinchilla'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('GET', 'root', 'Artichoke', 'Artichoke'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('GET', 'root', 'Mozzerella', 'Mozzerella'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('GET', 'root', 'Spinnach', 'Spinnach'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('GET', 'root', 'Alfredo', 'Alfredo'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('GET', 'root', 'Komquat', 'Komquat'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('GET', 'root', 'Rosemary', 'Rosemary'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('GET', 'root', 'Shrimp', 'Shrimp'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('GET', 'root', 'Halibut', 'Halibut'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('GET', 'root', 'Corn', 'Corn'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('GET', 'root', 'Yams', 'Yams'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('GET', 'root', 'Horseraddish', 'Horseraddish'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('GET', 'root', 'Garlic', 'Garlic'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('GET', 'root', 'Cauliflower', 'Cauliflower'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('GET', 'root', 'Pasta', 'Pasta'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('GET', 'root', 'Mushroom', 'Mushroom'))
# time.sleep(0.25)
# TEST 1 END----------------------------------

# TEST 2: NODE LEAVING NETWORK----------------
# nodes = []
# for i in range(0,6):
#     nodes.append(ChordNode(queues))
#
# for n in nodes:
#     n.start()
#     time.sleep(0.25)
#
# queues[nodes[0].id].put(Message('SET', 'root', 'Chinchilla', 'Chinchilla'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Artichoke', 'Artichoke'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Mozzerella', 'Mozzerella'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Spinnach', 'Spinnach'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Alfredo', 'Alfredo'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Komquat', 'Komquat'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Rosemary', 'Rosemary'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Shrimp', 'Shrimp'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Halibut', 'Halibut'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Corn', 'Corn'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Yams', 'Yams'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Horseraddish', 'Horseraddish'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Garlic', 'Garlic'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Cauliflower', 'Cauliflower'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Pasta', 'Pasta'))
# time.sleep(0.25)
# queues[nodes[0].id].put(Message('SET', 'root', 'Mushroom', 'Mushroom'))
# time.sleep(0.25)
#
# for n in nodes:
#     n.report()
#     time.sleep(0.25)
#
# print('---------------------------------------------------')
# print('---------------SENDING LEAVE MESSAGE---------------')
# print('---------------------------------------------------')
#
# queues[nodes[0].id].put(Message('LEAVE_NETWORK', 'root', None, None))
# time.sleep(0.25)
# queues[nodes[2].id].put(Message('LEAVE_NETWORK', 'root', None, None))
# time.sleep(0.25)
# queues[nodes[3].id].put(Message('LEAVE_NETWORK', 'root', None, None))
# time.sleep(0.25)
#
# for n in nodes:
#     n.report()
#     time.sleep(0.25)
# TEST 2 END----------------------------------

# TEST 3: NODE LEAVING NETWORK + RESPONSIVE FINGER FIXING------
nodes = []
for i in range(0,5):
    nodes.append(ChordNode(queues))

for n in nodes:
    n.start()
    time.sleep(0.25)

queues[nodes[0].id].put(Message('SET', 'root', 'Chinchilla', 'Chinchilla'))
time.sleep(0.25)
queues[nodes[0].id].put(Message('SET', 'root', 'Artichoke', 'Artichoke'))
time.sleep(0.25)
queues[nodes[0].id].put(Message('SET', 'root', 'Mozzerella', 'Mozzerella'))
time.sleep(0.25)
queues[nodes[0].id].put(Message('SET', 'root', 'Spinnach', 'Spinnach'))
time.sleep(0.25)
queues[nodes[0].id].put(Message('SET', 'root', 'Alfredo', 'Alfredo'))
time.sleep(0.25)
queues[nodes[0].id].put(Message('SET', 'root', 'Komquat', 'Komquat'))
time.sleep(0.25)
queues[nodes[0].id].put(Message('SET', 'root', 'Rosemary', 'Rosemary'))
time.sleep(0.25)
queues[nodes[0].id].put(Message('SET', 'root', 'Shrimp', 'Shrimp'))
time.sleep(0.25)
queues[nodes[0].id].put(Message('SET', 'root', 'Halibut', 'Halibut'))
time.sleep(0.25)
queues[nodes[0].id].put(Message('SET', 'root', 'Corn', 'Corn'))
time.sleep(0.25)
queues[nodes[0].id].put(Message('SET', 'root', 'Yams', 'Yams'))
time.sleep(0.25)
queues[nodes[0].id].put(Message('SET', 'root', 'Horseraddish', 'Horseraddish'))
time.sleep(0.25)
queues[nodes[0].id].put(Message('SET', 'root', 'Garlic', 'Garlic'))
time.sleep(0.25)
queues[nodes[0].id].put(Message('SET', 'root', 'Cauliflower', 'Cauliflower'))
time.sleep(0.25)
queues[nodes[0].id].put(Message('SET', 'root', 'Pasta', 'Pasta'))
time.sleep(0.25)
queues[nodes[0].id].put(Message('SET', 'root', 'Mushroom', 'Mushroom'))
time.sleep(0.25)

for n in nodes:
    n.report()
    time.sleep(0.25)

print('---------------------------------------------------')
print('---------------SENDING LEAVE MESSAGE---------------')
print('---------------------------------------------------')

queues[nodes[0].id].put(Message('LEAVE_NETWORK', 'root', None, None))
time.sleep(0.25)

for n in nodes:
    n.report()
    time.sleep(0.25)

queues[nodes[1].id].put(Message('SET', 'root', 'Curry', 'Curry'))
time.sleep(0.25)
queues[nodes[1].id].put(Message('SET', 'root', 'Milkshake', 'Milkshake'))
time.sleep(0.25)

for n in nodes:
    n.report()
    time.sleep(0.25)

# TEST 3 END----------------------------------

# TEST 4 FIXING FINGERS-----------------------
# Testing Code
# nodes = []
#
# for i in range(0, 6):
#     nodes.append(ChordNode(None, None, None, queues))
#
# print('STARTING NODES')
#
# for n in nodes:
#     n.start()
#     time.sleep(1)
#
# print('FIXING FINGERS')
#
# for n in nodes:
#     # n.fix_all_fingers()
#     queues[n.id].put(Message('FIX_FINGERS', None, 0, None, None))
#     time.sleep(1)
# #
# print('REPORTING STATE')
#
# for n in nodes:
#     n.report()
#     time.sleep(1)
# TEST 4 -------------------------------------


running = False
