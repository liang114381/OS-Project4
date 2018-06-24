import hashlib
import random
import os

def getHashString(s):
    return hashlib.sha256(s.encode('utf8')).hexdigest()

def checkHash(hash):
    return hash[0:5] == "00000"

def generateUUID():
    u = list(os.urandom(16))
    u[6] = (u[6] | 0x40) & 0x4F
    u[8] = (u[8] | 0x80) & 0xBF
    u = bytes(u)
    return u.hex()

def generateNonce():
    return "{:08}".format(random.randint(0, 99999999))

def checkUserId(userId):
    return isinstance(userId, str) and len(userId) == 8