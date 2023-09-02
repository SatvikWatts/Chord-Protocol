# ChordProtocol implmentation using gRPC Python

Peer-to-peer systems and applications are distributed systems without any centralized control or hierarchical organization, in which each node runs software with equivalent functionality.

The Chord protocol supports just one operation: given a key,it maps the key onto a node. 
Depending on the application using Chord, that node might be responsible for storing a value associated with the key. Chord uses consistent hashing to assign keys to Chord nodes. Consistent hashing tends to balance load,
since each node receives roughly the same number of keys, and requires relatively little movement of keys when nodes join and leave the system.

