import db_pb2_grpc
import db_pb2
from utils import *
import grpc
import logging, random, time
from db import DBEngine, Transaction, Block, BlockChain
from threading import Lock, Condition, Thread, Timer
from repeated_timer import RepeatedTimer


class MinerThreading(Thread):
    def __init__(self):
        Thread.__init__(self)

        self.block = None
        self.success = True

        self.lock = Lock()
        self.cond = Condition(self.lock)

    def setBlock(self, block):
        block.updateNonce(generateNonce())
        # print(block.json)
        with self.lock:
            self.block = block
            self.success = False
            self.cond.notify()

    def run(self):
        while(True):
            with self.lock:
                while self.success:
                    self.cond.wait()
                nonce = generateNonce()
                self.block.updateNonce(nonce)
                if checkHash(self.block.hash):
                    logging.debug('succeeded to produce a block!')
                    self.success = True


class BlockChainServer(db_pb2_grpc.BlockChainMinerServicer):

    def __init__(self, serverId, addr, peers, dataDir):
        super()

        if not os.path.exists(dataDir):
            os.makedirs(dataDir)

        self.serverId = serverId
        self.addr = addr
        self.database = DBEngine(dataDir)
        self.miner = MinerThreading()
        self.daemonTimer = RepeatedTimer(0.5, self.daemon)

        self.peers = peers
        self.stubs = []
        for peer in self.peers:
            logging.debug('added peer {}'.format(peer))
            channel = grpc.insecure_channel(peer)
            self.stubs.append(db_pb2_grpc.BlockChainMinerStub(channel))
            self.stubs[-1].addr = peer

        self.miner.daemon = True
        self.miner.start()
        self.daemonTimer.start()

    def daemon(self):
        if self.miner.block is not None and self.miner.success:
            self.database.addBlock(self.miner.block)

        if len(self.database.curChain) != 0:
            for stub in self.stubs:
                stub.PushBlock(db_pb2.JsonBlockString(Json=self.database.curChain[-1].toJson()))

        block = self.database.packToBlock()
        if block is not None:
            self.miner.setBlock(block)

    def Get(self, request, context):
        value = self.database.get(request.UserID)
        return db_pb2.GetResponse(Value=value)

    def Transfer(self, request, context):
        res = self.database.pushTransaction(Transaction(request))
        for stub in self.stubs:
            stub.PushTransaction(request)
        return db_pb2.BooleanResponse(Success=res)

    def Verify(self, request, context):
        res = self.database.verify(Transaction(request))
        if res == 'SUCCEEDED':
            return db_pb2.VerifyResponse.Results.SUCCEEDED
        elif res == 'PENDING':
            return db_pb2.VerifyResponse.Results.PENDING
        else:
            return db_pb2.VerifyResponse.Results.FAILED

    def PushTransaction(self, request, context):
        self.database.pushTransaction(Transaction(request))
        return db_pb2.Null()

    def PushBlock(self, request, context):
        blockJsonString = request.Json
        block = Block(blockJsonString)
        self.database.addBlock(block)

        last = block

        while True:
            if last.PrevHash == BlockChain.NullHash or last.PrevHash in self.database.blocks:
                break
            found = False
            for stub in self.stubs:
                J = stub.GetBlock(db_pb2.GetBlockRequest(BlockHash=last.PrevHash)).Json
                logging.debug("get block from {} with hash {}".format(stub.addr, last.PrevHash[:6]))
                if J == "":
                    continue
                reqBlock = Block(J)
                self.database.addBlock(reqBlock)
                last = reqBlock
                found =True
                break
            if not found:
                break

        self.database.updateLongestBlockChain()

        return db_pb2.Null()

    def GetHeight(self, request, context):
        self.database.updateLongestBlockChain()
        return db_pb2.GetHeightResponse(Height=len(self.database.curChain),
                                        LeafHash=self.database.curChain[-1].hash)

    def GetBlock(self, request, context):
        return db_pb2.GetBlockRequest(BlockHash=self.database.getBlockByHash(request.BlockHash))