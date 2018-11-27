# chordsim
A simulation of a Chord distributed hash table in Python.

This implementation moreless follows the structure laid out by Yimei Liao here:
http://www.qucosa.de/fileadmin/data/qucosa/documents/5227/data/HS_P2P_Liao.pdf
Their example uses a 3-bit identifier space, whereas this implementation is 8 bit.
Their diagrams are still very helpful for understanding how each node uses their fingering
table to find the node responsible for storing each key.

Chord partitions their keyspace according to an identifier circle (see the diagrams linked above).
Each point on the circle represents a key, and every node is identified by a single key on this circle.
A node stores all the keys between it's identifier key and the key of the closest node behind it on
the circle.  

Nodes of the Chord DHT are simulated using threads.  Each contains the following:

ID             # ID both uniquely identifies this node and specifies the last key in its portion of the identifier circle.
successor      # The ID of the next node in the identifier space.
predecessor    # The ID of the previous node in the identifier space.
finger_table   # A table of finger objects that point to other nodes in the identifier space.
queues         # Queues for messaging other nodes.  Stored in a dictionary.
hash_table     # The hash table part of the distributed hash table.

From here forward, n will be used to refer to both the node and the node's ID interchangably.
When a node n receives a GET or SET request for it hashes the filename to determine the target ID, then checks if
the ID falls within:
Case 1) the keyspace of n
Case 2) the keyspace of n.successor
Case 3) the keyspace of some other node

In case 1, n simply SETs or GETs the value stored at ID from it's local hash table.
In case 2, n forwards the query to the message queue of it's immediate successor.
In case 3, n searches for the closest predecessor to ID in its fingering table.  It then
forwards the query to the message queue of that predecessor.

The fingering table is sorted by increasing distance, but when searching, n iterates over it in reverse.
For each finger f, n checks to see if (n <= f <= ID) is satisfied.  If so, it f is sent the GET or SET request.


Normally the fingering tables are created when the node joins the DHT system, but for now they are manually created
in the testing code.  These are laid out readably in chord.py.
