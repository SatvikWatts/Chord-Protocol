from server import DHT
import dht_pb2 as dht
import dht_pb2_grpc as rpc
from client import Client
import time
import threading
import logging
import sys
import os


def main():

    # c1 = Client('localhost:50051')
    # c2 = Client('localhost:50052')
    # c3 = Client('localhost:50053')

    # c1.join(c2.address)
    # c2.join(c1.address)
    # c3.join(c1.address)

    # while True:
    #     time.sleep(4)
    #     c1.getPredecessor()
    #     c1.getSuccessor()

    #     c2.getPredecessor()
    #     c2.getSuccessor()

    #     c3.getPredecessor()
    #     c3.getSuccessor()

    # time.sleep(1000)

    c1 = Client('localhost:50051')
    c2 = Client('localhost:50052')
    c3 = Client('localhost:50053')
    c4 = Client('localhost:50054')
    c5 = Client('localhost:50055')

    # Join the nodes together to form a ring.
    c1.join(c2.address)
    time.sleep(2)
    time.sleep(2)
    c2.join(c1.address)
    time.sleep(2)
    c3.join(c1.address)
    time.sleep(2)
    c4.join(c1.address)
    time.sleep(2)
    c5.join(c1.address)
    time.sleep(2)

    time.sleep(15)

    # c1.fixFingers()

    # Store and retrieve key-value pairs in the DHT.
    data1 = dht.KeyValue(key='foo', value='bar')
    data2 = dht.KeyValue(key='hello', value='world')
    data3 = dht.KeyValue(key='yo', value='bitch')
    data4 = dht.KeyValue(key='ashok', value='malhotra')
    data5 = dht.KeyValue(key='tyler', value='durden')

    c1.store(data1)
    c2.store(data2)
    c3.store(data3)
    c3.store(data4)
    c3.store(data5)

    print('Now')
    time.sleep(10)
    c1.getPredecessor()
    c1.getSuccessor()

    c2.getPredecessor()
    c2.getSuccessor()

    c3.getPredecessor()
    c3.getSuccessor()

    c4.getPredecessor()
    c4.getSuccessor()

    c5.getPredecessor()
    c5.getSuccessor()

    print('------------------------------------------')

    time.sleep(10)

    # c2.leave(c2.node_info)
    # time.sleep(5)
    # c5.leave(c1.node_info)

    response = c3.retrieve(dht.KeyValue(key='foo'))
    print(response)

    response = c4.retrieve(dht.KeyValue(key='hello'))
    print(response)

    response = c5.retrieve(dht.KeyValue(key='yo'))
    print(response)

    response = c3.retrieve(dht.KeyValue(key='ashok'))
    print(response)

    response = c4.retrieve(dht.KeyValue(key='vaibhav'))
    if response.value == '':
        print('Key not in the network')

    while True:

        time.sleep(10)
        # c1.getPredecessor()
        # c1.getSuccessor()

        # c2.getPredecessor()
        # c2.getSuccessor()

        c3.getPredecessor()
        c3.getSuccessor()

        c4.getPredecessor()
        c4.getSuccessor()

        c5.getPredecessor()
        c5.getSuccessor()

    # Leave the nodes from the DHT.

    print('waiting for termination')
    while True:
        pass


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            # stopThreads()
            sys.exit(130)
        except SystemExit:
            # stopThreads()
            os._exit(130)
