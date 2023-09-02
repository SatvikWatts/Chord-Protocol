
import logging
import grpc
import dht_pb2 as dht
import dht_pb2_grpc as rpc
from concurrent import futures
import sys
import hashlib
import threading

KEY_SIZE = 10


class Client:

    def __init__(self, address) -> None:
        self.address = address
        self.node_info = dht.NodeInfo(
            id=str(self.__hash(self.address)), address=str(self.address))
        self.channel = grpc.insecure_channel(address)
        self.stub = rpc.DHTStub(self.channel)

        try:
            self.getPredecessor()
        except grpc.RpcError as e:
            logging.info(e)

        try:
            self.getSuccessor()
        except grpc.RpcError as e:
            logging.info(e)

    def join(self, node):
        # logging.info(node)
        node = dht.NodeInfo(id=str(self.__hash(node)), address=str(node))
        self.stub.join(node)

    def store(self, data):
        return self.stub.store(data)

    def retrieve(self, data):
        return self.stub.retrieve(data)

    def __hash(self, key):
        # Hashes a key to an integer between 0 and 2^KEY_SIZE-1.
        key_bytes = key.encode('utf-8')
        hash_bytes = hashlib.sha1(key_bytes).digest()
        return int.from_bytes(hash_bytes, byteorder='big') % (2**KEY_SIZE)

    def stabilize(self):
        self.stub.stabilize(dht.Empty())

    def notify(self, node):
        self.stub.notify(node)

    def fixFingers(self):
        try:
            self.stub.fixFingers(dht.Empty())
        except grpc.RpcError as e:
            logging.info('Error in fixFingers(C)', e.details())

    def getPredecessor(self):
        try:
            val = self.stub.getPredecessor(dht.Empty()).id
            if val == '':
                val = -1
            logging.info(
                'Pred of {id} - {pred}'.format(id=int(self.__hash(self.address)), pred=int(val)))
        except grpc.RpcError as e:
            logging.info('Node Left')

        else:
            t = threading.Timer(10, self.getPredecessor)
            t.start()

    def getSuccessor(self):
        try:
            val = self.stub.getSuccessor(dht.Empty()).id
            if val == '':
                val = -1
            logging.info(
                'Succ of {id} - {succ}'.format(id=int(self.__hash(self.address)), succ=int(val)))
        except grpc.RpcError as e:
            logging.info('Node Left')
        else:
            t = threading.Timer(10, self.getSuccessor)
            t.start()

    def leave(self, node=None):
        if node is None:
            node = self.node_info

        self.stub.leave(node)
