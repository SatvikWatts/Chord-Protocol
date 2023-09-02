import grpc
import hashlib
import logging
import threading
from concurrent import futures
import sys
import os
import dht_pb2 as dht
import dht_pb2_grpc as rpc


# The number of bits in a key (for the hash function).
KEY_SIZE = 10

# length of successor list
r = 5


class DHT(rpc.DHTServicer):

    def __init__(self,  address):

        self.address = address
        self.port = address[10:]
        self.node_id = self.__hash(self.address)
        self.data = {}
        self.node_info = dht.NodeInfo(
            id=str(self.node_id), address=self.address)
        self.successor = None
        self.predecessor = None
        self.fingerTable = [None]*(KEY_SIZE+1)
        self.successorList = [None]*(r)
        self.next = 0

        # periodically stabilising node
        try:
            self.stabilize()
        except grpc.RpcError as e:
            logging.info('Error in stabilising')

        # updating fingertable in regular intervals
        try:
            self.fixFinger()
        except grpc.RpcError as e:
            logging.info('Error in updating fingerTable')

        # updating fingertable in regular intervals
        try:
            self.updateSuccessorList()
        except grpc.RpcError as e:
            logging.info('Error in updating successor List')

        # checking whether predecessor has failed in regular intervals
        try:
            self.checkPredecessor()
        except grpc.RpcError as e:
            logging.info('Error in updating successor List')

        # checking whether successor has failed in regular intervals
        try:
            self.checkSuccessor()
        except grpc.RpcError as e:
            logging.info('Error in updating successor List')

        # starting server
        logging.info('{%s} : %s', self.port, str(self.node_id))
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        rpc.add_DHTServicer_to_server(self, self.server)
        self.server.add_insecure_port(address)
        self.server.start()
        logging.info('Server Started')
        self.server.wait_for_termination()

    def stopServer(self):
        self.server.stop()

    def __hash(self, key):
        # Hashes a key to an integer between 0 and 2^KEY_SIZE-1.
        key_bytes = key.encode('utf-8')
        hash_bytes = hashlib.sha1(key_bytes).digest()
        return int.from_bytes(hash_bytes, byteorder='big') % (2**KEY_SIZE)

    def __is_between(self, key, start, end):
        """
        Check if the given key is between start and end in a circular space.
        """
        if start == end:
            return True
        if start < end:
            return key > start and key <= end
        else:
            return key > start or key <= end

    def join(self, request, context=None):
        """
        Join the DHT by contacting an existing node and updating its predecessor and successor.
        """
        # logging.info('{%s} : Request recvd',self.port)
        # logging.info('{%s} : Call from %s %s',self.port,request.id,request.address)
        node_info = dht.NodeInfo(
            id=str(self.__hash(request.address)), address=str(request.address))
        if self.successor is None:
            # logging.info(self.node_id)
            self.successor = node_info
            self.predecessor = node_info

            logging.info(
                f"{self.port} :Node {self.address} joined the DHT as the first node")
        else:
            if self.__is_between(int(node_info.id), int(self.__hash(self.address)), int(self.__hash(self.successor.address))):
                node_channel = grpc.insecure_channel(request.address)
                node_stub = rpc.DHTStub(node_channel)

                try:
                    node_stub.join(self.node_info)
                except grpc.RpcError as e:
                    logging.info('{} Status = {}'.format(
                        self.node_id, e.details()))
                else:
                    self.successor = node_info
                    # logging.info(f"Node {self.address} joined the DHT with successor {self.successor.address}")
            else:
                
                node_channel = grpc.insecure_channel(self.successor.address)
                node_stub = rpc.DHTStub(node_channel)

                try:
                    node_stub.join(request)
                except grpc.RpcError as e:
                    logging.info('Status = {}'.format(e.details()))

        return dht.NodeInfo(id=str(self.__hash(self.address)), address=str(self.address))

    def leave(self, request, context):
        node_info = request
        logging.info('{%s} : Node %s leaving network', self.port, node_info.id)

        if int(node_info.id) == int(self.node_id):

            # updating predecessor.successor

            # logging.info('pred {}'.format(self.predecessor))
            pred_channel = grpc.insecure_channel(self.predecessor.address)
            pred_stub = rpc.DHTStub(pred_channel)

            try:
                pred_stub.updateSuccessor(self.successor)
            except grpc.RpcError as e:
                logging.info('Status = {}'.format(e.details()))

            # updating successor.predecessor

            succ_channel = grpc.insecure_channel(self.successor.address)
            succ_stub = rpc.DHTStub(succ_channel)

            try:
                succ_stub.updatePredecessor(self.predecessor)
            except grpc.RpcError as e:
                logging.info('Status = {}'.format(e.details()))

            # If this node is leaving, update its successor and predecessor.
            self.successor = None
            self.predecessor = None

            self.server.stop(10)
            logging.info('{%s} : Node %s left network',
                         self.port, node_info.id)
            return dht.Empty()

        # Forward the request to the successor.
        successor_channel = grpc.insecure_channel(self.successor.address)
        successor_stub = rpc.DHTStub(successor_channel)

        try:
            successor_stub.leave(node_info)
        except grpc.RpcError as e:
            logging.info('Status = {}'.format(e.details()))

        return dht.Empty()

    def store(self, request, context):
        key = request.key
        value = request.value
        logging.info('{%s} : Storing key %s at node %s',
                     self.port, key, self.node_id)

        # Hash the key to an integer.
        key_hash = self.__hash(key)

        if not self.successor:
            # If this node has no successor, store the value here.
            self.data[key_hash] = value
            logging.info('{%s} : Stored key %s with id %s at node %s',
                         self.port, key, str(key_hash), self.node_id)
            return dht.KeyValue(key='', value='')

        # Hash the IDs to integers.

        successor_id = int(self.successor.id)
        current_id = int(self.node_id)

        # Determine whether the key should be stored here or forwarded to the successor.
        if self.__is_between(int(key_hash), current_id, successor_id) == True:
            successor_channel = grpc.insecure_channel(self.successor.address)
            successor_stub = rpc.DHTStub(successor_channel)

            try:
                successor_stub.insertData(request)
            except grpc.RpcError as e:
                logging.info('Status = {}'.format(e.details()))
            # self.data[key_hash] = value

        else:

            # successor_channel = grpc.insecure_channel(self.successor.address)
            # successor_stub = rpc.DHTStub(successor_channel)

            n0 = self.closestPrecedingNode(key_hash)
            successor_channel = grpc.insecure_channel(n0.address)
            successor_stub = rpc.DHTStub(successor_channel)
            # logging.info('check')
            try:
                successor_stub.store(request)
            except grpc.RpcError as e:
                logging.info('Status = {}'.format(e.details()))

        return dht.KeyValue(key='', value='')

    def retrieve(self, request, context):
        key = request.key
        logging.info('{%s} : Retrieving key %s', self.port, key)

        # Hash the key to an integer.
        key_hash = self.__hash(key)

        if not self.successor:
            # If this node has no successor, the key is not in the DHT.
            return dht.KeyValue(key='', value='')

        # Hash the IDs to integers.
        successor_id = self.successor.id
        current_id = self.node_id

        # Determine whether the key is stored here or should be retrieved from the successor.
        if self.__is_between(int(key_hash), int(current_id), int(successor_id)):
            successor_channel = grpc.insecure_channel(self.successor.address)
            successor_stub = rpc.DHTStub(successor_channel)

            try:
                value = successor_stub.retrieveData(request)
            except grpc.RpcError as e:
                logging.info('Status = {}'.format(e.details()))
            else:
                return value
        else:
            # n0 = self.closestPrecedingNode(request)
            # successor_channel = grpc.insecure_channel(self.successor.address)
            # successor_stub = rpc.DHTStub(successor_channel)

            n0 = self.closestPrecedingNode(key_hash)
            successor_channel = grpc.insecure_channel(n0.address)
            successor_stub = rpc.DHTStub(successor_channel)


            try:
                value = successor_stub.retrieve(request)
            except grpc.RpcError as e:
                logging.info('Status = {}'.format(e.details()))
            else:
                return value

        return dht.KeyValue(key='', value='')

    def findSuccessor(self, request, context):

        if self.successor is not None:

            # logging.info('{%s} : Call from %s %s',self.port,request.id,request.address)
            if self.__is_between(int(request.id), int(self.node_id), int(self.successor.id)):
                # logging.info('{%s} : successor of %s is %s ',self.port,request.id,self.successor.id)
                return self.successor
            else:
                n0 = self.closestPrecedingNode(request.id)
                successor_channel = grpc.insecure_channel(n0.address)
                successor_stub = rpc.DHTStub(successor_channel)

                try:
                    val = successor_stub.findSuccessor(request)
                except grpc.RpcError as e:
                    logging.info(
                        'Status in findSuccessor = {}'.format(e.details()))
                
                return val
            

        return dht.NodeInfo(id='', address='')

    def closestPrecedingNode(self, request):
        

        for i in range(KEY_SIZE, 1, -1):
            if self.fingerTable[i] is not None:
                if self.__is_between(int(self.fingerTable[i].id), int(self.node_id), int(request)) == True:
                    return self.fingerTable[i]

        return self.node_info

    def stabilize(self):

        if self.successor is not None:
            sucessor_channel = grpc.insecure_channel(self.successor.address)
            sucessor_stub = rpc.DHTStub(sucessor_channel)

            try:
                x = sucessor_stub.getPredecessor(dht.Empty())
            except grpc.RpcError as e:
                logging.info('Status in stabilise= {}'.format(e.details()))
            else:
                # logging.info('{%s}:stabilize function %s' ,self.port,x.id)

                if x.id != '' and x.id != self.node_id:
                    if self.__is_between(int(x.id), int(self.node_id), int(self.successor.id)) == True:
                        self.successor = x

                        #replicating data
                        sucessor_channel = grpc.insecure_channel(self.successor.address)
                        sucessor_stub = rpc.DHTStub(sucessor_channel)
                        for key in self.data.keys():
                            data = dht.KeyValue(key = str(key),value = str(self.data[key]))
                            sucessor_stub.replicate1(data)

                    sucessor_channel = grpc.insecure_channel(self.successor.address)
                    sucessor_stub = rpc.DHTStub(sucessor_channel)

                try:
                    sucessor_stub.notify(self.node_info)
                except grpc.RpcError as e:
                    logging.info(
                        'Status in stabilise = {}'.format(e.details()))

        self.stabilizeTimer = threading.Timer(1.0, self.stabilize)

        try:
            self.stabilizeTimer.start()
        except grpc.RpcError as e:
            logging.info('Error in stabilising')

    def notify(self, request, context):

        if self.successor is not None:
            if self.predecessor == None or self.__is_between(int(request.id), int(self.predecessor.id), int(self.successor.id)) == True:
                self.predecessor = request

        return dht.Empty()

    def fixFinger(self):

        if self.successor is not None:
            self.next = self.next + 1
            if self.next > KEY_SIZE:
                self.next = 1

            channel = grpc.insecure_channel(self.address)
            stub = rpc.DHTStub(channel)
            node = dht.NodeInfo(
                id=str(int(self.node_id) + 2**(self.next-1)), address='')
            
            val = dht.NodeInfo(id = '',address = '')
            try:
                val = stub.findSuccessor(node)
            except grpc.RpcError as e:
                logging.info('Status in fiXFinger = {}'.format(e.details()))
            
            if val.id != '':
                self.fingerTable[self.next] = val
            else:
                self.next = self.next - 1

        self.fingerTimer = threading.Timer(2.0, self.fixFinger)

        try:
            self.fingerTimer.start()
        except grpc.RpcError as e:
            logging.info('Error in updating finger table')

    def getPredecessor(self, request, context):
        if self.predecessor is not None:
            return self.predecessor

        return dht.NodeInfo(id='', address='')

    def getSuccessor(self, request, context):
        if self.successor is not None:
            return self.successor

        return dht.NodeInfo(id='', address='')

    def insertData(self, request, context):
        key_hash = self.__hash(request.key)
        self.data[key_hash] = request.value

        # first immediate successor
        if self.successorList[0] is not None:
            succ_channel = grpc.insecure_channel(self.successorList[0].address)
            succ_stub = rpc.DHTStub(succ_channel)

            try:
                succ_stub.replicate(request)
            except grpc.RpcError as e:
                logging.info('Status = {}'.format(e.details()))

        # second immediate successor
        if len(self.successorList) > 1 and self.successorList[1] is not None:
            succ_channel = grpc.insecure_channel(self.successorList[1].address)
            succ_stub = rpc.DHTStub(succ_channel)

            try:
                succ_stub.replicate(request)
            except grpc.RpcError as e:
                logging.info('Status = {}'.format(e.details()))

        logging.info('{%s} : Stored key %s with id %s at node %s',
                     self.port, request.key, str(key_hash), self.node_id)

        return dht.Empty()

    def retrieveData(self, request, context):
        key_hash = self.__hash(request.key)
        # logging.info('key_hash: %s in %s', str(key_hash), str(self.port))

        logging.info(self.data)
        if key_hash in self.data.keys():
            logging.info('key_hash: %s in %s', str(key_hash), str(self.port))
            return dht.KeyValue(key=request.key, value=self.data[key_hash])

        return dht.KeyValue(key='', value='')

    def replicate(self, request, context):
        key_hash = self.__hash(request.key)
        self.data[key_hash] = request.value

        logging.info('{%s} :  Stored key %s with id %s at node %s',
                     self.port, request.key, str(key_hash), self.node_id)

        return dht.Empty()

    def replicate1(self,request,context):
        key_hash = int(request.key)
        self.data[key_hash] = request.value

        logging.info('{%s} :  Stored key %s with id %s at node %s',
                     self.port, request.key, str(key_hash), self.node_id)

        return dht.Empty()


    def updatePredecessor(self, request, context):
        self.predecessor = request
        return dht.Empty()

    def updateSuccessor(self, request, context):
        self.successor = request
        return dht.Empty()

    def getSuccessorList(self, request, context):
        if self.successor is not None:
            self.successorList[0] = self.successor

        if self.successorList[0] is not None:
            info = dht.SuccList()
            for ss in self.successorList:
                if ss is not None:
                    info.vt.append(ss)

            return info

        return dht.SuccList()

    def ping(self, request, context):
        return dht.Empty()

    def updateSuccessorList(self):

        flag = False

        for succ in self.successorList:
            if succ is None:
                flag = True
                break

        if flag == False and len(self.successorList) == (r-1):
            return

        if self.successor is not None:
            succ_channel = grpc.insecure_channel(self.successor.address)
            succ_stub = rpc.DHTStub(succ_channel)

            if self.successorList[0] is None:
                self.successorList[0] = self.successor

            try:
                list = succ_stub.getSuccessorList(dht.Empty())

                if list.vt:
                    temp = []
                    for i in range(len(list.vt)):
                        if list.vt[i] == self.node_info:
                            break
                        temp.append(list.vt[i])
                    temp.insert(0, self.successor)
                    self.successorList = temp
                    while len(self.successorList) > r:
                        self.successorList.pop()

            except grpc.RpcError as e:
                logging.info(
                    'Status in updateSuccessorList= {}'.format(e.details()))

        self.Timer = threading.Timer(2.0, self.updateSuccessorList)

        try:
            self.Timer.start()
        except grpc.RpcError as e:
            logging.info('Error in updating successor list')

    def checkSuccessor(self):
        # logging.info('SuccList',self.successorList)
        if self.successorList[0] is not None:
            succ_channel = grpc.insecure_channel(self.successorList[0].address)
            succ_stub = rpc.DHTStub(succ_channel)
            try:
                succ_stub.ping(dht.Empty())
            except grpc.RpcError as e:
                self.successorList.pop(0)
                self.successorList.append(None)
                self.successor = self.successorList[0]

                # logging.info('Updated Successor',self.successor)

        self.Timer = threading.Timer(3.0, self.checkSuccessor)

        try:
            self.Timer.start()
        except grpc.RpcError as e:
            logging.info('Error in checking Successor')

    def checkPredecessor(self):
        if self.predecessor is not None:
            pred_channel = grpc.insecure_channel(self.predecessor.address)
            pred_stub = rpc.DHTStub(pred_channel)
            try:
                pred_stub.ping(dht.Empty())
            except grpc.RpcError as e:
                self.predecessor = None

        self.Timer = threading.Timer(4.0, self.checkPredecessor)

        try:
            self.Timer.start()
        except grpc.RpcError as e:
            logging.info('Error in checking Predecessor')


if __name__ == '__main__':
    # logging.basicConfig(filename = 'logs.log',level=logging.INFO,filemode='a')
    logging.basicConfig(level=logging.INFO)

    try:
        server = DHT(sys.argv[1])
    except KeyboardInterrupt:
        logging.info('Interrupted')
        try:
            sys.exit(130)
        except SystemExit:
            os._exit(130)
