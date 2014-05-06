#Distributed Key-Value Storage

## Usage

./kv_storage [process ID] [delay time to replica 1] [delay time to replica 2] [delay time to replica 3]

## Configurations

Modify 'config.py' in 'distributed-key-value-store' directory accordingly.
'config['hosts']' is a list of (IP address, server port, client port).

## Algorithm

The logical ring has size m = 11.
When a client enters a command from the terminal associated with key 'Key', a hash function hashes 'Key' into a hash value 'Key_hash'.
The client then finds the node which immediately succeeds 'Key_hash' in the ring and sends the message to that node.
This node will act as the coordinator for that particular key. After the coordinator receives the message from the client, it
"forwards" the message to the three replicas (for our implementation, the coordinator is not the replica for that key). Depending on
the consistency level, the coordinator will wait for all the replicas before responding to the client or just wait for one.
If the 'get' command was sent by the client, the coordinator initiates a read repair for that key in the background.

Randomized delay was implemented by waiting certain amount of time (range specified by the arguments of kv_storage) before
sending the message to the replicas.

For our purpose, the servers/clients processes are numbered from 0 to 3, assuming 4 servers in total. The following is the has values for
the processes.

Process 0: 115 \\
Process 1: 32 \\
Process 2: 900 \\
Process 3: 63

So the immediate successor for the key with hash value 35 would be Process 3 and the immediate successor for the key with hash value 1000
is Process 1.

## Requirement

Python 3
