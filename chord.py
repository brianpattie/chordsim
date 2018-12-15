from hashlib import blake2b
import threading
import queue
import struct
import time
import random
import math

# TEST = 0 # - THE "I HOPE I DIDN'T BREAK ANYTHING I CAN'T FIX" TEST
# TEST = 1 # - PARTITION PASSING DURING NODE JOIN
# TEST = 2 # - NODE LEAVING NETWORK
TEST = 3 # - NODE LEAVING NETWORK + RESPONSIVE FINGER FIXING
# TEST = 4 # - CONCURRENT NODE DEPARTURES

TIMEOUT = 2     # How long a message
HASH_BITS = 8   # The number of bits in each ID on the identifier circle.

running = True              # Set to false at the end of testing code to stop the simulation
random.seed(time.time())    # Seed random number generation


def hash(file_name):                            # Returns a the unsigned integer representation of 1 byte hash of the input string
    h = blake2b(digest_size=1)                  # Used BLAKE2 because it allows you to specify the size of the hash
    h.update(file_name.encode())
    return struct.unpack('B', h.digest())[0]


def mod_between(i, lb, ub):                 # Returns true if i is between lower bound lb and upper bound ub
    if lb < ub and i >= lb and i <= ub:
        return True
    if lb > ub and (i >= lb or i <= ub):
        return True
    return False


class Message():    # Little more than a struct with a copy function

    def __init__(self, type, sender_id, mode=None, file_name=None, file_id=None, file_data=None, node=None, finger_num=None):
        self.type           = type          # Identifies what kind of message this is
        self.orig_sender_id = sender_id     # This should always equal the ID of the Chord Node that received a GET or SET request.  Messages are copied and passed a lot, this is where it started.
        self.sender_id      = sender_id     # The most recent node to copy and send this message.
        self.mode           = mode          # Further identifies what kind of message this in.  Needed since GET, SET, and Finger Update all use the same set of message types.
        self.file_name      = file_name     # The file name being looked up or stored.
        self.file_id        = file_id       # An ID, usually the hash of the file name, but occasionally just a point on the identifier circle when updating a finger.
        self.file_data      = file_data     # The file data.
        self.node           = node          # The ID of a node that's important in the message context.  Could be a successor, a predecessor, or a node that a finger should point to.
        self.finger_num     = finger_num    # The index of the finger that needs to be updated.

    # Returns a new message with the same values
    def copy(self):
        return Message(self.type, self.orig_sender_id, mode=self.mode, file_name=self.file_name, file_id = self.file_id, file_data = self.file_data, node = self.node, finger_num = self.finger_num)

    def print(self):
        print('---type:')
        print(self.type)
        print('---mode:')
        print(self.mode)
        print('---orig_sender_id:')
        print(self.orig_sender_id)
        print('---sender_id:')
        print(self.sender_id)
        print('---file_id:')
        print(self.file_id)
        print('---file_data:')
        print(self.file_data)
        print('---node:')
        print(self.node)
        print('---finger_num:')
        print(self.finger_num)


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

        rand_queue = queue_keys[random.randint(0, len(queue_keys)) - 1]                             # Select a random node.
        self.queues[rand_queue].put(Message('FIND_PRED', self.id, mode='INIT', file_id=self.id))    # Ask it to tell you your predecessor. #FIXME
        msg = self.wait_for_message_type('FIND_PRED_RESULT')                                        # Wait for the result and buffer any messages you get in the meantime.  You'll answer them when you are live.
        self.predecessor = msg.node

        self.queues[self.predecessor].put(Message('FIND_SUCC', self.id, mode='INIT', file_id=self.id))   # Ask your predecessor for your successor. #FIXME
        msg = self.wait_for_message_type('FIND_SUCC_RESULT')                                             # Its current successor is your successor unless two nodes joined right next to eachother, in which case this code is not smart enough to handle it.
        self.finger_table.append(Finger(self.id + 1, msg.node))                                          # Store your seccessor in your first (and closest) finger.

        i = 1
        while i < HASH_BITS:                                # Only the successor needs to be set for the node to function.  Set all fingers to the successor, and do the updates live.
            start = (self.id + 2**i) % 2**HASH_BITS         # <start> is an ID some distance away
            succ = self.finger_table[0].node                # <succ> is the successor of <start>, but it will be updated once this node is live.  For now it's set to your successor.
            Finger(start, succ)                             # Each finger on the table should point to an ID twice as far as the one before it.
            self.finger_table.append(Finger(start, succ))   # The last finger will point to the ID on the opposite side of the identifier circle.
            i += 1

        self.update_required = True                         # Set this flag true so that you will update your fingers once you go live.

        self.stabalize()                                                                # Your neigbours should point at you.  Tell them you exist.
        self.queues[self.finger_table[0].node].put(Message('DATA_REQUEST', self.id))    # Ask your successor for your share of it's partition.


    def run(self):  # After initializing, thread execution starts here.

        self.join_network();    # Join the network by initializing your predecessor and finger values.  Also tell the neighbouring nodes to point at you.
        print('Node: ' + str(self.id) + ' has joined the network')

        while running:  # Main loop

            if self.update_required:        # This will be true if you tried to send a message to a node that doesn't exist, or you just joined the network.
                self.ask_for_fingers()      # Dispatch messages to find your true fingers.

            if len(self.msg_buf) > 0:
                msg = self.msg_buf.pop(0)                               # Read any messages you buffered while you were waiting for a specific message.
            else:
                try:
                    msg = self.queues[self.id].get(timeout = TIMEOUT)   # Wait some time for a message.  This timeouts so that the node can perform stabalizations, but I didn't get to that.  Left it in.
                except:
                    continue


            if msg.type == 'GET_REQUEST':           # If you have the value they are looking for, print it.  Otherwise pass it along.
                self.get(msg)

            elif msg.type == 'SET_REQUEST':         # If the key falls in your partition, set it to your local hash table.  Otherwise look up the node it belongs to.
                self.set(msg)

            elif msg.type == 'FIND_PRED':           # A node is looking for a predecessor to some ID.  If it's you, reply with a FIND_PRED_RESULT message.  Otherwise pass it along.
                self.remote_find_predecessor(msg)

            elif msg.type == 'FIND_PRED_RESULT':    # A predecessor node you were looking for returned a result.  Ask it for it's successor.
                self.find_successor(msg)

            elif msg.type == 'FIND_SUCC':           # A node is asking for your successor.  Return a message with your successor's ID.
                self.remote_find_successor(msg)

            elif msg.type == 'FIND_SUCC_RESULT':    # You've been given the ID of a successor you were looking for.  Do things depending on what the original request was.
                if msg.mode == 'GET':                   # You were responding to a GET_REQUEST.  Forward the GET_REQUEST to the node where the ID belongs.
                    self.forward_get(msg)
                elif msg.mode == 'SET':                 # You were responding to a SET_REQUEST.  Forward the SET_REQUEST to the node where the ID belongs.
                    self.forward_set(msg)
                elif msg.mode == 'FINGER':              # You were updating your fingers.  Use the data in the message to update one of your fingers.
                    self.update_finger(msg)

            elif msg.type == 'SUCC_STABALIZE':      # A node has joined the network and it thinks it's your predecessor.  Check the ID it sent you and update accordingly.
                self.succ_stabalize(msg)

            elif msg.type == 'PRED_STABALIZE':      # A node has joined the network and it thinks it's your successor.  Check the ID it sent you and update accordingly.
                self.pred_stabalize(msg)

            elif msg.type == 'SET_SUCCESSOR':       # A node has left the network and it's telling you that you have a new successor.  Update it!
                self.set_successor(msg)

            elif msg.type == 'SET_PREDECESSOR':     # A node has left the network and it's telling you that you have a new predecessor.  Update it!
                self.set_predecessor(msg)

            elif msg.type == 'DATA_REQUEST':        # A node has joined the network and is now responsible for some of your partition.  Send them their share!
                self.send_partition_data(msg)

            elif msg.type == 'DATA_TRANSFER':       # A node has sent some data that should be in your partition.  Store it!
                self.insert_data(msg)

            elif msg.type == 'LEAVE_NETWORK':       # Its time to get out of here.  Send your data to your successor and let your neighbours know you've left.
                self.leave_network()

            msg = None

        print('Node ' + str(self.id) + ' exiting')  # End
        exit()


    def get(self, msg):                                                                                 # Hash the filename
        id = hash(msg.file_name)                                                                        # Check if the ID falls in your partition...
        if mod_between(id, self.predecessor + 1, self.id):                                              #       It does.  Store it locally.
            print('Node ' + str(self.id) + ' performed GET on ' + str(msg.file_name) + ' (Hashes to ' + str(id) + ') retrieving value: ' + str(self.hash_table[id]))
        else:
            new_msg = Message('FIND_PRED', self.id, mode='GET', file_name=msg.file_name, file_id=id)    #       It doesn't.  First step is to find the ID's predecessor.
            self.queues[self.closest_preceding_finger(id)].put(new_msg)                                 #       Send the message along.  It will come back and invoke a different function.

    def forward_get(self, msg):             # You received a GET_REQUEST a while ago.  The ID doesn't belong to you, but by now you've identified which node is responsible for it.
        new_msg = msg.copy()                # Copy the message.  It contains the file_name.
        new_msg.type = 'GET_REQUEST'        # Change the message type.  You're forwarding the GET_REQUEST that you received earlier.
        new_msg.sender_id = self.id         # Set yourself as the most recent sender.
        self.queues[msg.node].put(new_msg)  # Send the message along.


    # Writes data to the DHT under hash(file_name)
    def set(self, msg):
        id = hash(msg.file_name)                                                                                                # Hash the filename
        if mod_between(id, self.predecessor + 1, self.id):                                                                      # Check if the ID falls in your partition...
            self.hash_table[id] = msg.file_data                                                                                 #       It does.  Store it locally.
            print('Node ' + str(self.id) + ' performed SET on ' + str(msg.file_name) + ' (Hashes to ' + str(id) + ') setting value: ' + str(self.hash_table[id]))
        else:
            new_msg = Message('FIND_PRED', self.id, mode='SET', file_name=msg.file_name, file_id=id, file_data=msg.file_data)   #       It doesn't.  First step is to find the ID's predecessor.
            self.queues[self.closest_preceding_finger(id)].put(new_msg)                                                         #       Send the message along.  It will come back and invoke a find_successor.


    def forward_set(self, msg):             # You received a SET_REQUEST a while ago.  The ID doesn't belong to you, but by now you've identified which node is responsible for it.
        new_msg = msg.copy()                # Copy the message.  It contains the file_name and file_data.
        new_msg.type = 'SET_REQUEST'        # Change the message type.  You're forwarding the SET_REQUEST that you received earlier.
        new_msg.sender_id = self.id         # Set yourself as the most recent sender.
        self.queues[msg.node].put(new_msg)  # Send the message along.


    def find_successor(self, msg):                      # Your request to find the ID's predecessor has come back.  Now it's time to get the ID of it's successor.
        sender_id = msg.sender_id                       # Copy the sender ID so you don't lose it.
        new_msg = msg.copy()                            # Copy the message.  It contains all the data needed to process this request, so it must be maintained.
        new_msg.sender_id = self.id                     # Set yourself as the most recent sender (but leave orig_sender_id alone.  It was you, but seriously, don't touch it).
        new_msg.type = 'FIND_SUCC'                      # Change the message type.  You're asking for a successor now.
        self.queues[sender_id].put(new_msg)             # Send the message back to the ID's predecessor.

    def remote_find_successor(self, msg):               # You've received a request for the ID of your successor
        new_msg = msg.copy()                            # Copy the message.  It contains all the data needed to process this request, so it must be maintained.
        new_msg.sender_id = self.id                     # Set yourself as the most recent sender (but leave orig_sender_id alone).
        new_msg.type = 'FIND_SUCC_RESULT'               # Change the message type to a result.
        new_msg.node = self.finger_table[0].node        # Add your successor's ID to the message.
        self.queues[msg.orig_sender_id].put(new_msg)    # Return it to the original sender.


    def remote_find_predecessor(self, msg):                                         # You've received a request to find the predecessor of an ID.
        new_msg = msg.copy()                                                        # Copy the message.  It contains all the data needed to process this request, so it must be maintained.
        new_msg.sender_id = self.id                                                 # Set yourself as the most recent sender (but leave orig_sender_id alone).
        if mod_between(msg.file_id, self.id + 1, self.finger_table[0].node):        # If this node is the predecessor...
            new_msg.type = 'FIND_PRED_RESULT'                                       #       Change the message type to a result.
            new_msg.node = self.id                                                  #       Add your ID to the message.
            self.queues[msg.orig_sender_id].put(new_msg)                            #       Return it to the original sender.
        else:                                                                       # If this node is NOT the predecessor...
            self.queues[self.closest_preceding_finger(msg.file_id)].put(new_msg)    #       Pass the message along to the closest preceding finger to the ID in the message.


    def closest_preceding_finger(self, id):
        for f in reversed(self.finger_table):                                       # Go through my finger table, starting with the furthest finger.
            if mod_between(f.node, self.id + 1, id) and self.node_exists(f.node):   # Check if the node this finger points to is behind the ID I'm looking for.
                return f.node                                                       # If so, send to this node.
        return self.id                                                              # If none of my fingers are behind it, then it must be me.  This should never happen.


    def node_exists(self, id):                                                                                              # Check if the node is still in the network
        if id in self.queues.keys():                                                                                        # It's queue still exists, so it is.
            return True
        else:                                                                                                               # It's queue does not exist.  He's gone.
            print('Node: ' + str(self.id) + ' could not send message to ' + str(id) + ' because there was no queue for it')
            self.update_required = True                                                                                     # I'll update my fingers so it doesn't happen again next time.
            return False


    def stabalize(self):                                                                                    # I need to make sure that my neighbours are pointing at me.
        if self.id != self.finger_table[0].node:                                                            # If I'm not my own successor... (only node in the network)
            self.queues[self.finger_table[0].node].put(Message('SUCC_STABALIZE', self.id, node=self.id))    #       Tell my successor to check if I'm its predecessor.
        if self.id != self.predecessor:                                                                     # If I'm not my own predecessor... (only node in the network)
            self.queues[self.predecessor].put(Message('PRED_STABALIZE', self.id, node=self.id))             #       Tell my predecessor to check if I'm its successor.

    def succ_stabalize(self, msg):                                                                          # A new node thinks its my predecessor.  Check if it's right.
        if self.id == self.predecessor or mod_between(msg.node, self.predecessor, self.id):                 # It was right.
            self.predecessor = msg.node                                                                     #       Update my predecessor.
        else:                                                                                               # It was wrong.
            self.queues[msg.orig_sender_id].put(Message('SET_SUCCESSOR', self.id, node=self.predecessor))   #       Tell it that it's actually the predecessor of the node behind me.


    def pred_stabalize(self, msg):                                                                                      # A new node thinks its my successor.  Check if it's right.
        if self.id == self.finger_table[0].node or mod_between(msg.node, self.id, self.finger_table[0].node):           # It was right.
            self.finger_table[0].node = msg.node                                                                        #       Update my successor.
        else:                                                                                                           # It was wrong.
            self.queues[msg.orig_sender_id].put(Message('SET_PREDECESSOR', self.id, node=self.finger_table[0].node))    #       Tell it that it's actually the successor of the node in front of me.


    def set_successor(self, msg):
        self.finger_table[0].node = msg.node    # My successor left, but he told me who my new successor is.


    def set_predecessor(self, msg):
        self.predecessor = msg.node     # My predecessor left, but he told me who my new predecessor is.


    def report(self):                                               # Used for testing, nothing to see here.
        print('------------------------------------------')
        print('Node ID: ' + str(self.id))
        print('Predecessor: ' + str(self.predecessor))
        for f in range(0, len(self.finger_table)):
            print('Finger ' + str(f) + ': starts at ' + str(self.finger_table[f].start) + ' and points at Node ' + str(self.finger_table[f].node))
        print('Local Hash Table Contents:')
        for id in list(self.hash_table.keys()):
            print(str(id) + '\t= ' + str(self.hash_table[id]))


    def update_finger(self, msg):
        if TEST == 3:   # Only print during test 3.  Too much clutter.
            print('Node: ' + str(self.id) + ' updating finger ' + str(msg.finger_num) + ' (start = ' + str(msg.file_id) + ') to point at node: ' + str(msg.node))
        self.finger_table[msg.finger_num].node = msg.node   # Your finger update request came back.  Update your finger with the node in the message.


    def ask_for_fingers(self):
        if TEST == 3:   # Only print during test 3.  Too much clutter.
            print('Node ' + str(self.id) + ' is asking for finger updates')
        for i in range(1, len(self.finger_table)):                                                                                                          # For all of your fingers...
            if mod_between(self.finger_table[i].start, self.id, self.finger_table[0].node):                                                                 # Check if it should point to your successor.
                self.finger_table[i].node = self.finger_table[0].node                                                                                       #       It should point at my successor.  No need to ask questions.
            else:
                self.queues[self.finger_table[0].node].put(Message('FIND_PRED', self.id, mode='FINGER', finger_num=i, file_id=self.finger_table[i].start))  #       It is not my successor.  Send a message out to find it.
        self.update_required = False                                                                                                                        # Gotta reset that flag or we'll come striaght back here.


    def send_partition_data(self, msg):
        for id in list(self.hash_table.keys()):                                                                                         # Send your data to your new predecessor.
            if not mod_between(id, self.predecessor + 1, self.id):                                                                      # Only send the data that falls in their partition.
                self.queues[msg.orig_sender_id].put(Message('DATA_TRANSFER', self.id, file_id=id, file_data=self.hash_table[id]))       # Send the data.
                self.hash_table.pop(id, None)                                                                                           # Delete your copy.  Gotta let it fly.


    def relinquish_partition_data(self):
        for id in list(self.hash_table.keys()):                                                                                         # Give all your data to your successor.
            print('Node ' + str(self.id) + ' giving away ' + str(id) + ' before I die.')
            self.queues[self.finger_table[0].node].put(Message('DATA_TRANSFER', self.id, file_id=id, file_data=self.hash_table[id]))    # Send a piece of data.
            self.hash_table.pop(id, None)                                                                                               # Delete your copy.  Its important to let go of the past.


    def insert_data(self, msg):
        self.hash_table[msg.file_id] = msg.file_data    # Someone sent you data.  You should trust them and store it here without doing any kind of validation.


    def wait_for_message_type(self, type):
        msg = self.queues[self.id].get()    # Get a message from your queue.
        while msg.type != type:             # This is not the message I wanted.
            self.msg_buf.append(msg)        # Keep getting messages till you get the one you are expecting.
        return msg


    def leave_network(self):
        self.queues[self.finger_table[0].node].put(Message('SET_PREDECESSOR', self.id, node=self.predecessor))  # Tell your successor that your predecessor is now its predecessor.
        self.queues[self.predecessor].put(Message('SET_SUCCESSOR', self.id, node=self.finger_table[0].node))    # Tell your predecessor that your successor is now its successor.
        del self.queues[self.id]                                                                                # Delete your message queue so you won't get any more pesky messages.
        self.relinquish_partition_data()                                                                        # Send your data to your successor before you disappear forever.
        print('Node ' + str(self.id) + ' leaving network and exiting')                                          # Say goodbye!
        exit()


# Main
# UNIVERSAL TEST SET UP
queues = {}
queues['root'] = queue.Queue()

# TEST 0: THE "I HOPE I DIDN'T BREAK ANYTHING I CAN'T FIX" TEST---------
if TEST == 0:
    nodes = []
    nodes.append(ChordNode(queues))
    nodes[0].start()

    for n in nodes:
        n.report()
        time.sleep(0.25)

    nodes.append(ChordNode(queues))
    nodes.append(ChordNode(queues))
    nodes.append(ChordNode(queues))
    nodes.append(ChordNode(queues))

    for i in range(1, len(nodes)):
        nodes[i].start()
        time.sleep(0.25)

    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Chinchilla', file_data='Chinchilla'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Artichoke', file_data='Artichoke'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Mozzerella', file_data='Mozzerella'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Spinnach', file_data='Spinnach'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Alfredo', file_data='Alfredo'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Komquat', file_data='Komquat'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Rosemary', file_data='Rosemary'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Shrimp', file_data='Shrimp'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Halibut', file_data='Halibut'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Corn', file_data='Corn'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Yams', file_data='Yams'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Horseraddish', file_data='Horseraddish'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Garlic', file_data='Garlic'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Cauliflower', file_data='Cauliflower'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Pasta', file_data='Pasta'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Mushroom', file_data='Mushroom'))

    for n in nodes:
        n.report()
        time.sleep(0.25)

    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Chinchilla'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Artichoke'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Mozzerella'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Spinnach'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Alfredo'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Komquat'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Rosemary'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Shrimp'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Halibut'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Corn'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Yams'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Horseraddish'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Garlic'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Cauliflower'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Pasta'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Mushroom'))

    time.sleep(0.25)
# TEST 0 END -----------------------

# TEST 1: PARTITION PASSING DURING NODE JOIN---
if TEST == 1:
    nodes = []
    nodes.append(ChordNode(queues))
    nodes[0].start()

    time.sleep(0.25)

    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Chinchilla', file_data='Chinchilla'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Artichoke', file_data='Artichoke'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Mozzerella', file_data='Mozzerella'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Spinnach', file_data='Spinnach'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Alfredo', file_data='Alfredo'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Komquat', file_data='Komquat'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Rosemary', file_data='Rosemary'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Shrimp', file_data='Shrimp'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Halibut', file_data='Halibut'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Corn', file_data='Corn'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Yams', file_data='Yams'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Horseraddish', file_data='Horseraddish'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Garlic', file_data='Garlic'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Cauliflower', file_data='Cauliflower'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Pasta', file_data='Pasta'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Mushroom', file_data='Mushroom'))

    time.sleep(0.25)
    print('---------------------------------------------------')

    # Add New Nodes now that there is data in the first node
    new_nodes = []
    for i in range(0,5):
        new_nodes.append(ChordNode(queues))
        # time.sleep(1)

    for n in new_nodes:
        n.start()
        time.sleep(1)

    print('---------------------------------------------------')
    print('FINAL VALUES')

    nodes[0].report()
    for n in new_nodes:
        n.report()
        time.sleep(0.25)

    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Chinchilla'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Artichoke'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Mozzerella'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Spinnach'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Alfredo'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Komquat'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Rosemary'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Shrimp'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Halibut'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Corn'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Yams'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Horseraddish'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Garlic'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Cauliflower'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Pasta'))
    queues[nodes[0].id].put(Message('GET_REQUEST', 'root', file_name='Mushroom'))
# TEST 1 END----------------------------------

# TEST 2: NODE LEAVING NETWORK----------------
if TEST == 2:
    nodes = []
    for i in range(0,6):
        nodes.append(ChordNode(queues))

    for n in nodes:
        n.start()
        time.sleep(0.25)

    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Chinchilla', file_data='Chinchilla'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Artichoke', file_data='Artichoke'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Mozzerella', file_data='Mozzerella'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Spinnach', file_data='Spinnach'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Alfredo', file_data='Alfredo'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Komquat', file_data='Komquat'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Rosemary', file_data='Rosemary'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Shrimp', file_data='Shrimp'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Halibut', file_data='Halibut'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Corn', file_data='Corn'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Yams', file_data='Yams'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Horseraddish', file_data='Horseraddish'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Garlic', file_data='Garlic'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Cauliflower', file_data='Cauliflower'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Pasta', file_data='Pasta'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Mushroom', file_data='Mushroom'))

    time.sleep(0.25)

    for n in nodes:
        n.report()
        time.sleep(0.25)

    print('---------------------------------------------------')
    print('---------------SENDING LEAVE MESSAGEs--------------')
    print('---------------------------------------------------')

    queues[nodes[0].id].put(Message('LEAVE_NETWORK', 'root', None, None))
    time.sleep(0.25)
    queues[nodes[2].id].put(Message('LEAVE_NETWORK', 'root', None, None))
    time.sleep(0.25)
    queues[nodes[3].id].put(Message('LEAVE_NETWORK', 'root', None, None))
    time.sleep(0.25)

    for n in nodes:
        n.report()
        time.sleep(0.25)

    print('---------------------------------------------------')
# TEST 2 END----------------------------------

# TEST 3: NODE LEAVING NETWORK + RESPONSIVE FINGER FIXING------
if TEST == 3:
    nodes = []
    for i in range(0,10):
        nodes.append(ChordNode(queues))

    for n in nodes:
        n.start()
        time.sleep(0.25)

    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Chinchilla', file_data='Chinchilla'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Artichoke', file_data='Artichoke'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Mozzerella', file_data='Mozzerella'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Spinnach', file_data='Spinnach'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Alfredo', file_data='Alfredo'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Komquat', file_data='Komquat'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Rosemary', file_data='Rosemary'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Shrimp', file_data='Shrimp'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Halibut', file_data='Halibut'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Corn', file_data='Corn'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Yams', file_data='Yams'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Horseraddish', file_data='Horseraddish'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Garlic', file_data='Garlic'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Cauliflower', file_data='Cauliflower'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Pasta', file_data='Pasta'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Mushroom', file_data='Mushroom'))

    time.sleep(0.25)

    for n in nodes:
        n.report()
        time.sleep(0.25)

    print('---------------------------------------------------')
    print('---------------SENDING LEAVE MESSAGE---------------')
    print('---------------------------------------------------')

    queues[nodes[0].id].put(Message('LEAVE_NETWORK', 'root', None, None))
    time.sleep(0.25)
    queues[nodes[2].id].put(Message('LEAVE_NETWORK', 'root', None, None))
    time.sleep(0.25)
    queues[nodes[4].id].put(Message('LEAVE_NETWORK', 'root', None, None))
    time.sleep(0.25)
    queues[nodes[6].id].put(Message('LEAVE_NETWORK', 'root', None, None))
    time.sleep(0.25)

    print('---------------------------------------------------')

    for n in nodes:
        n.report()
        time.sleep(0.25)

    print('---------------------------------------------------')

    queues[nodes[1].id].put(Message('GET_REQUEST', 'root', file_name='Chinchilla'))
    queues[nodes[3].id].put(Message('GET_REQUEST', 'root', file_name='Artichoke'))
    queues[nodes[5].id].put(Message('GET_REQUEST', 'root', file_name='Mozzerella'))
    queues[nodes[7].id].put(Message('GET_REQUEST', 'root', file_name='Spinnach'))
    queues[nodes[1].id].put(Message('GET_REQUEST', 'root', file_name='Alfredo'))
    queues[nodes[3].id].put(Message('GET_REQUEST', 'root', file_name='Komquat'))
    queues[nodes[5].id].put(Message('GET_REQUEST', 'root', file_name='Rosemary'))
    queues[nodes[7].id].put(Message('GET_REQUEST', 'root', file_name='Shrimp'))
    queues[nodes[1].id].put(Message('GET_REQUEST', 'root', file_name='Halibut'))
    queues[nodes[3].id].put(Message('GET_REQUEST', 'root', file_name='Corn'))
    queues[nodes[5].id].put(Message('GET_REQUEST', 'root', file_name='Yams'))
    queues[nodes[7].id].put(Message('GET_REQUEST', 'root', file_name='Horseraddish'))
    queues[nodes[1].id].put(Message('GET_REQUEST', 'root', file_name='Garlic'))
    queues[nodes[3].id].put(Message('GET_REQUEST', 'root', file_name='Cauliflower'))
    queues[nodes[5].id].put(Message('GET_REQUEST', 'root', file_name='Pasta'))
    queues[nodes[7].id].put(Message('GET_REQUEST', 'root', file_name='Mushroom'))

    time.sleep(0.25)

    print('---------------------------------------------------')

    for n in nodes:
        n.report()
        time.sleep(0.25)
# TEST 3 END----------------------------------

# TEST 4 CONCURRENT NODE DEPARTURES-----------------------
if TEST == 4:
    nodes = []
    for i in range(0,10):
        nodes.append(ChordNode(queues))

    for n in nodes:
        n.start()
        time.sleep(0.25)

    print('---------------------------------------------------')

    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Chinchilla', file_data='Chinchilla'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Artichoke', file_data='Artichoke'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Mozzerella', file_data='Mozzerella'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Spinnach', file_data='Spinnach'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Alfredo', file_data='Alfredo'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Komquat', file_data='Komquat'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Rosemary', file_data='Rosemary'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Shrimp', file_data='Shrimp'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Halibut', file_data='Halibut'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Corn', file_data='Corn'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Yams', file_data='Yams'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Horseraddish', file_data='Horseraddish'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Garlic', file_data='Garlic'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Cauliflower', file_data='Cauliflower'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Pasta', file_data='Pasta'))
    queues[nodes[0].id].put(Message('SET_REQUEST', 'root', file_name='Mushroom', file_data='Mushroom'))

    time.sleep(0.25)

    for n in nodes:
        n.report()
        time.sleep(0.25)

    print('---------------------------------------------------')
    print('---------------SENDING LEAVE MESSAGE---------------')
    print('---------------------------------------------------')

    for i in range(0,9):
        queues[nodes[i].id].put(Message('LEAVE_NETWORK', 'root', None, None))

    print('---------------------------------------------------')

    for n in nodes:
        n.report()
        time.sleep(0.25)
# TEST 4 -------------------------------------

running = False
