import db_pb2
import json
from utils import *
import threading
import logging
import time


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
            self.BlockID = int(block.BlockID)
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
            self.time = None
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
        self.time = None


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
                logging.info('loaded block {}'.format(block.BlockID))
                cur += 1
            except IOError:
                break

    def append(self, block, writeToFile=True):
        if len(self.chain) == 0:
            if block.PrevHash != self.NullHash or block.BlockID != 1:
                return False
        else:
            if self.chain[-1].hash != block.PrevHash or block.BlockID != len(self.chain) + 1:
                return False

        tmpBalances = Balances(0)
        transSet = set()
        for trans in block.Transactions:
            if trans.UUID in transSet or trans.UUID in self.transDepth:
                return False
            transSet.add(trans.UUID)
            if trans.Value < trans.MiningFee or trans.MiningFee <= 0:
                return False
            if not checkUserId(trans.FromID) or not checkUserId(trans.ToID):
                return False
            tmpBalances[trans.FromID] -= trans.Value
            tmpBalances[trans.ToID] += trans.Value - trans.MiningFee
            tmpBalances[block.MinerID] += trans.MiningFee
            if self.balances[trans.FromID] + tmpBalances[trans.FromID] < 0 \
                    or self.balances[trans.ToID] + tmpBalances[trans.ToID] < 0:
                return False

        self.chain.append(block)
        for trans in block.Transactions:
            self.transDepth[trans.UUID] = block.BlockID
            self.balances[trans.FromID] -= trans.Value
            self.balances[trans.ToID] += trans.Value - trans.MiningFee
            self.balances[block.MinerID] += trans.MiningFee

        if self.dataDir is not None and writeToFile:
            with open(os.path.join(self.dataDir, str(block.BlockID) + '.json'), 'w') as f:
                f.write(block.toJson())
        return True

    def writeToFile(self, dataDir):
        # for f in glob.glob(os.path.join(dataDir, '*.json')):
        #     os.remove(f)
        for block in self.chain:
            filePath = os.path.join(dataDir, str(block.BlockID) + '.json')
            with open(filePath, 'w') as blockFile:
                blockFile.write(block.toJson())


class TransientLogManager:
    def __init__(self, dataDir):
        self.logDir = os.path.join(dataDir, 'transientLogs')
        if not os.path.isdir(self.logDir):
            os.makedirs(self.logDir)
        self.transientTransList = []
        self.transientTransByUUID = {}
        self.updateLock = threading.Lock()

        for filePath in os.listdir(self.logDir):
            with open(os.path.join(self.logDir, filePath), 'r') as f:
                lines = f.readlines()
                trans = Transaction(lines[0])
                trans.time = float(lines[1])
                self.transientTransList.append(trans)
                self.transientTransByUUID[trans.UUID] = trans
        self.transientTransList.sort(key=lambda t: t.time)

    def logTransfer(self, trans):
        with self.updateLock:
            if trans.UUID in self.transientTransByUUID:
                return False
            self.transientTransList.append(trans)
            self.transientTransByUUID[trans.UUID] = trans
            logFilePath = self.getLogFilePath(trans.UUID)
            with open(logFilePath, 'w') as f:
                f.write(trans.toJson().strip() + '\n' + str(time.time()))
            return True

    def getLogFilePath(self, UUID):
        return os.path.join(self.logDir, UUID + '.log')

    def removeTransfer(self, UUID):
        with self.updateLock:
            if UUID in self.transientTransByUUID:
                self.transientTransList.remove(self.transientTransByUUID[UUID])
                del self.transientTransByUUID[UUID]
                try:
                    logFilePath = self.getLogFilePath(UUID)
                    os.remove(logFilePath)
                    logging.debug('removed transient log file {}'.format(logFilePath))
                except IOError:
                    pass


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
        if self.transVerified(trans):
            return 'SUCCEEDED'
        if not self.transExist(trans) and \
            trans.UUID not in self.transManager.transientTransByUUID:
            return 'FAILED'
        return 'PENDING'

    def pushTransaction(self, trans):
        with self.updateLock:
            if trans.FromID == trans.ToID:
                return False
            if trans.MiningFee <= 0 or trans.MiningFee >= trans.Value:
                return False
            self.transManager.logTransfer(trans)
            return True

    def addBlock(self, block):
        with self.updateLock:
            self.blocks[block.hash] = block
            self.curChain.append(block)

    def removeBlock(self, block):
        with self.updateLock:
            del self.blocks[block.hash]

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
        with self.updateLock:
            while True:
                if len(self.blocks) == 0:
                    return
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
                if len(self.curChain) != 0 and selectedBlock.hash == self.curChain.chain[-1].hash:
                    return
                chain = []
                valid = True
                while valid:
                    chain.append(selectedBlock)
                    if selectedBlock.PrevHash == BlockChain.NullHash:
                        break
                    try:
                        prevBlock = self.blocks[selectedBlock.PrevHash]
                        if selectedBlock.BlockID != prevBlock.BlockID + 1:
                            valid = False
                        selectedBlock = prevBlock
                    except KeyError:
                        valid = False
                if not valid:
                    for block in chain:
                        self.removeBlock(block)
                else:
                    break

            self.curChain = BlockChain(self.dataDir, loadFromDir=False)
            for block in reversed(chain):
                self.curChain.append(block)

    def packToBlock(self, minerId):
        with self.updateLock:
            transactions = []
            tmpBalances = Balances(0)
            for trans in self.transManager.transientTransList:
                if not self.transExist(trans):
                    tmpBalances[trans.FromID] -= trans.Value
                    if self.curChain.balances[trans.FromID] + tmpBalances[trans.FromID] < 0:
                        tmpBalances[trans.FromID] += trans.Value
                        self.transManager.removeTransfer(trans.UUID)
                    tmpBalances[trans.ToID] += trans.Value - trans.MiningFee
                    tmpBalances[minerId] += trans.MiningFee
                    transactions.append(trans)
                    if len(transactions) >= self.TRANS_IN_BLOCK:
                        break
                elif self.transVerified(trans):
                    self.transManager.removeTransfer(trans.UUID)
            if len(transactions) == 0:
                return None
            block = Block()
            block.BlockID = len(self.curChain) + 1
            block.MinerID = minerId
            block.Transactions = transactions
            if len(self.curChain) == 0:
                block.PrevHash = BlockChain.NullHash
            else:
                block.PrevHash = self.curChain[-1].hash
            return block

    def transExist(self, trans):
        return trans.UUID in self.curChain.transDepth

    def transVerified(self, trans):
        return trans.UUID in self.curChain.transDepth and\
               self.curChain.transDepth[trans.UUID] <= len(self.curChain) - 6

    def blockByTrans(self, trans):
        return self.curChain[self.curChain.transDepth[trans.UUID] - 1]
