import db_pb2
import json
from utils import *
import threading
import logging
import sqlite3


class Balances(dict):
    def __init__(self, initialBalance):
        dict.__init__(self)
        self.initialBalance = initialBalance

    def __getitem__(self, key):
        if not dict.__contains__(self, key):
            dict.__setitem__(self, key, self.initialBalance)
        return dict.__getitem__(self, key)


class Block:

    def __init__(self, block=None):
        if isinstance(block, db_pb2.Block):
            self.BlockID = block.BlockID
            self.PrevHash = block.PrevHash
            self.MinerID = block.MinerID
            self.Nonce = block.Nonce
            self.Transactions = []
            for trans in block.Transactions:
                self.Transactions.append(Transaction(trans))
            self.json = self.toJson()
            self.hash = getHashString(self.json)
        elif isinstance(block, str):
            self.fromJson(block)
            self.json = block
            self.hash = getHashString(self.json)

    def toJson(self):
        Ts = []
        for trans in self.Transactions:
            Ts.append({
                'Type': 'TRANSFER',
                'FromID': trans.FromID,
                'ToID': trans.ToID,
                'Value': trans.Value,
                'MiningFee': trans.MiningFee,
                'UUID': trans.UUID
            })
        J = {
            'BlockID': self.BlockID,
            'PrevHash': self.PrevHash,
            'Transactions': Ts,
            'MinerID': self.MinerID,
            'Nonce': self.Nonce
        }
        return json.dumps(J)

    def fromJson(self, jsonString):
        J = json.loads(jsonString)
        self.BlockID = J['BlockID']
        self.PrevHash = J['PrevHash']
        self.MinerID = J['MinerID']
        self.Nonce = J['Nonce']
        self.Transactions = []
        self.hash = getHashString(jsonString)
        for trans in J['Transactions']:
            t = Transaction()
            t.FromID = trans['FromID']
            t.ToID = trans['ToID']
            t.Value = trans['Value']
            t.MiningFee = trans['MiningFee']
            t.UUID = trans['UUID']
            self.Transactions.append(t)

    def updateNonce(self, nonce):
        self.Nonce = nonce
        self.json = self.toJson()
        self.hash = getHashString(self.json)


class Transaction:

    def __init__(self, trans=None):
        if isinstance(trans, db_pb2.Transaction):
            self.Type = trans.Type
            self.FromID = trans.FromID
            self.ToID = trans.ToID
            self.Value = trans.Value
            self.MiningFee = trans.MiningFee
            self.UUID = trans.UUID
        elif isinstance(trans, str):
            self.fromJson(trans)

    def toJson(self):
        J = {
            'Type': self.Type,
            'FromID': self.FromID,
            'ToID': self.ToID,
            'Value': self.Value,
            'MiningFee': self.MiningFee,
            'UUID': self.UUID
        }
        return json.dumps(J)

    def fromJson(self, jsonString):
        J = json.loads(jsonString)
        self.Type = J['Type']
        self.FromID = J['FromID']
        self.ToID = J['ToID']
        self.Value = J['Value']
        self.MiningFee = J['Value']
        self.UUID = J['UUID']


class BlockChain:

    NullHash = "0000000000000000000000000000000000000000000000000000000000000000"

    def __init__(self, dataDir=None, loadFromDir=True):
        self.chain = []
        self.balances = Balances(1000)
        self.transDepth = {}
        self.dataDir = dataDir

        if dataDir is not None and loadFromDir:
            self.loadFromDir(dataDir)

    def __getitem__(self, key):
        return self.chain[key]

    def __len__(self):
        return len(self.chain)

    def loadFromDir(self, dir):
        cur = 1
        while True:
            try:
                f = open(os.path.join(dir, str(cur) + '.json'), 'r')
                block = Block(f.read())
                if not self.append(block, False):
                    break
                logging.debug('loaded block {}'.format(block.BlockID))
                cur += 1
            except IOError:
                break

    def append(self, block, writeToFile=True):
        if len(self.chain) == 0:
            if block.PrevHash != self.NullHash:
                return False
        else:
            if self.chain[-1].hash != block.PrevHash:
                return False

        tmpBalances = Balances(0)
        transSet = set()
        for trans in block.Transactions:
            if trans.UUID in transSet or trans.UUID in self.transDepth:
                return False
            transSet.add(trans.UUID)
            if trans.Value < trans.MiningFee or trans.MiningFee <=0:
                return False
            tmpBalances[trans.FromID] -= trans.Value
            tmpBalances[trans.ToID] += trans.Value - trans.MiningFee
            if self.balances[trans.FromID] + tmpBalances[trans.FromID] < 0 \
                    or self.balances[trans.ToID] + tmpBalances[trans.ToID] < 0:
                return False

        self.chain.append(block)
        for trans in block.Transactions:
            self.transDepth[trans.UUID] = block.BlockID
            self.balances[trans.FromID] -= trans.Value
            self.balances[trans.ToID] += trans.Value - trans.MiningFee

        if self.dataDir is not None and writeToFile:
            with open(os.path.join(self.dataDir, str(block.BlockID) + '.json'), 'w') as f:
                f.write(block.toJson())
        return True

    def transExist(self, trans):
        return trans.UUID in self.transDepth

    def writeToFile(self, dataDir):
        # for f in glob.glob(os.path.join(dataDir, '*.json')):
        #     os.remove(f)
        for block in self.chain:
            filePath = os.path.join(dataDir, str(block.BlockID) + '.json')
            with open(filePath, 'w') as blockFile:
                blockFile.write(block.toJson())


class TransientLogManager:
    def __init__(self, dataDir):
        self.logPath = os.path.join(dataDir, 'transient.log')
        self.transientTransList = []
        self.transientTransByUUID = {}

        if os.path.isfile(self.logPath):
            f = open(self.logPath, 'r')
            lines = f.readlines()
            for line in lines:
                trans = Transaction(line)
                self.transientTransList.append(trans)
                self.transientTransByUUID[trans.UUID] = trans
            f.close()

            self.logFile = open(self.logPath, 'a')
        else:
            self.logFile = open(self.logPath, 'w')

    def logTransfer(self, trans):
        self.transientTransList.append(trans)
        self.transientTransByUUID[trans.UUID] = trans
        self.logFile.write(trans.toJson().strip() + '\n')
        self.logFile.flush()


class DBEngine:

    TRANS_IN_BLOCK = 50

    def __init__(self, dataDir):
        self.dataDir = dataDir
        self.blocks = {}
        self.transManager = TransientLogManager(dataDir)
        self.blockBalances = Balances(1000)
        self.curChain = BlockChain(dataDir, loadFromDir=True)
        for block in self.curChain.chain:
            self.blocks[block.hash] = block

        self.updateLock = threading.Lock()

    def getChainByLeaf(self, leafStr: str):
        chain = []
        chain.append(Block(leafStr))

        bc = BlockChain()

        while chain[-1].hash != BlockChain.NullHash:
            chain.append(self.blocks[chain[-1].PrevHash])

        for block in reversed(chain):
            bc.append(block)

        return bc

    def get(self, userId):
        return self.curChain.balances[userId]

    def verify(self, trans):
        if trans.UUID in self.transManager.transientTransByUUID:
            return 'PENDING'
        if trans.UUID not in self.curChain.transDepth:
            return 'FAILED'
        if self.curChain.transDepth[trans.UUID] < len(self.curChain) - 6:
            return 'SUCCEEDED'
        return 'PENDING'

    def pushTransaction(self, trans):
        fromId = trans.FromID
        toId = trans.ToID
        value = trans.Value
        fee = trans.MiningFee
        # if self.transientBalances[fromId] < value:
        #     logging.debug("Transfer from {} to {} failed!".format(fromId, toId))
        #     return False
        if fromId == toId:
            logging.debug("Cannot transfer from one user to the same one!")
            return False
        # self.transientBalances[fromId] -= value
        # self.transientBalances[toId] += value - fee
        self.transManager.logTransfer(trans)

    def addBlock(self, block):
        self.blocks[block.hash] = block
        self.curChain.append(block)
        return True

    def getHeight(self):
        return len(self.curChain)

    def getBlockByHash(self, hash):
        if hash not in self.blocks:
            return ""
        return self.blocks[hash].toJson()

    def getDepthByBlockHash(self, blockHash):
        if blockHash not in self.blocks:
            return None
        return self.getBlockByHash(self.blocks[blockHash].BlockID)

    def updateLongestBlockChain(self):
        if len(self.blocks) == 0:
            return

        with self.updateLock:
            maxLength, minHash, selectedBlock = -1, None, None
            for hash, block in self.blocks.items():
                self.getDepthByBlockHash(block.hash)
                if block.BlockID > maxLength:
                    maxLength = block.BlockID
                    minHash = block.hash
                    selectedBlock = block
                elif block.BlockID == maxLength and block.hash < minHash:
                    minHash = block.hash
                    selectedBlock = block
            chain = []
            while True:
                chain.append(selectedBlock)
                if selectedBlock.PrevHash == BlockChain.NullHash:
                    break
                selectedBlock = self.blocks[selectedBlock.PrevHash]
            self.curChain = BlockChain(self.dataDir, loadFromDir=False)
            for block in reversed(chain):
                self.curChain.append(block)

    def packToBlock(self):
        transactions = []
        for trans in self.transManager.transientTransList:
            # print('len of curchain', len(self.curChain))
            if not self.curChain.transExist(trans):
                transactions.append(trans)
                if len(transactions) >= self.TRANS_IN_BLOCK:
                    break
        # print(len(transactions))
        if len(transactions) == 0:
            return None
        block = Block()
        block.BlockID = len(self.curChain) + 1
        block.MinerID = 'Server1'
        block.Transactions = transactions
        if len(self.curChain) == 0:
            block.PrevHash = BlockChain.NullHash
        else:
            block.PrevHash = self.curChain[-1].hash
        return block
