import db_pb2_grpc
import db_pb2
import grpc
import random
from utils import *


def get_id(id):
    return 'Test{:04}'.format(id)


def transfer(stub, fromId, toId, value, fee):
    trans = db_pb2.Transaction(Type=db_pb2.Transaction.TRANSFER,
                               FromID=fromId,
                               ToID=toId,
                               Value=value,
                               MiningFee=fee,
                               UUID=generateUUID())
    res = stub.Transfer(trans)

def get(stub, userId):
    req = db_pb2.GetRequest(UserID=userId)
    res = stub.Get(req)
    return res.Value

def verify(stub, trans):
    res = stub.Verify(trans)
    return res.Result, res.BlockHash[:6]

if __name__ == '__main__':
    channel = grpc.insecure_channel('127.0.0.1:50051')
    stub = db_pb2_grpc.BlockChainMinerStub(channel)
    for _ in range(100):
        transfer(stub, get_id(1), get_id(2), random.randint(2,15), 1)
    print(get(stub, get_id(1)))
    print(get(stub, get_id(2)))