import db_pb2_grpc
import db_pb2
from utils import *
import grpc
import logging, json, time
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
                    logging.info('succeeded to produce block{}!'.format(self.block.BlockID))
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
            logging.info('added peer {}'.format(peer))
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
                try:
                    stub.PushBlock(db_pb2.JsonBlockString(Json=self.database.curChain[-1].toJson()))
                except grpc.RpcError:
                    pass

        block = self.database.packToBlock('Server{:02}'.format(self.serverId))
        if block is not None:
            self.miner.setBlock(block)

    def Get(self, request, context):
        logging.info("query balance of {}".format(request.UserID))
        value = self.database.get(request.UserID)
        return db_pb2.GetResponse(Value=value)

    def Transfer(self, request, context):
        if not self.database.pushTransaction(Transaction(request)):
            return db_pb2.BooleanResponse(Success=False)
        logging.info("transfer {} from {} to {}".format(request.Value, request.FromID, request.ToID))
        success = False
        for stub in self.stubs:
            try:
                stub.PushTransaction(request, timeout=10)
                success = True
            except grpc.RpcError:
                continue
        return db_pb2.BooleanResponse(Success=success)

    def Verify(self, request, context):
        trans = Transaction(request)
        logging.info("verify transfer ({} from {} to {})".format(request.Value, request.FromID, request.ToID))
        res = self.database.verify(Transaction(request))
        if res == 'SUCCEEDED':
            return db_pb2.VerifyResponse(Result=db_pb2.VerifyResponse.SUCCEEDED,
                                         BlockHash=self.database.blockByTrans(trans).hash)
        elif res == 'PENDING':
            return db_pb2.VerifyResponse(Result=db_pb2.VerifyResponse.PENDING,
                                         BlockHash=self.database.blockByTrans(trans).hash)
        else:
            return db_pb2.VerifyResponse(Result=db_pb2.VerifyResponse.FAILED,
                                         BlockHash="")

    def PushTransaction(self, request, context):
        self.database.pushTransaction(Transaction(request))
        return db_pb2.Null()

    def PushBlock(self, request, context):
        blockJsonString = request.Json
        try:
            block = Block(blockJsonString)
        except (json.JSONDecodeError, AttributeError):
            return db_pb2.Null()
        if len(block.MinerID) != 8 or \
            block.MinerID[:6] != 'Server' or \
            not block.MinerID[6:8].isdigit() or \
            not checkHash(block.hash):
            return db_pb2.Null()

        self.database.addBlock(block)

        last = block

        while True:
            if last.PrevHash == BlockChain.NullHash or last.PrevHash in self.database.blocks:
                break
            found = False
            for stub in self.stubs:
                try:
                    logging.info("acquire block from {} with hash {}".format(stub.addr, last.PrevHash[:12]))
                    J = stub.GetBlock(db_pb2.GetBlockRequest(BlockHash=last.PrevHash), timeout=10).Json
                    if J == "":
                        continue
                    reqBlock = Block(J)
                except (grpc.RpcError, json.JSONDecodeError, AttributeError):
                    continue
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
        length = len(self.database.curChain)
        if length == 0:
            hash = ""
        else:
            hash = self.database.curChain[-1].hash
        time.sleep(2)
        return db_pb2.GetHeightResponse(Height=length, LeafHash=hash)

    def GetBlock(self, request, context):
        return db_pb2.GetBlockRequest(BlockHash=self.database.getBlockByHash(request.BlockHash))