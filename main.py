import json
import argparse
import grpc
import logging
import time
from concurrent import futures
from server import BlockChainServer
import db_pb2_grpc

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Run some server')
    parser.add_argument('--id', action='store')
    args = parser.parse_args()
    serverId = args.id

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s, %(levelname)s: Server{} %(message)s'.format(serverId))

    config = json.loads(open('config.json').read())

    nServers = config['nservers']
    peers = []
    for i in range(1, nServers+1):
        if str(i) == str(serverId):
            continue
        # print(i, serverId)
        peers.append('{}:{}'.format(config[str(i)]['ip'], config[str(i)]['port']))

    service = BlockChainServer(serverId,
                               '{}:{}'.format(config[str(serverId)]['ip'], config[str(serverId)]['port']),
                               peers, config[str(serverId)]['dataDir'])

    threadPool = futures.ThreadPoolExecutor(max_workers=10)
    grpcServer = grpc.server(threadPool)
    db_pb2_grpc.add_BlockChainMinerServicer_to_server(service, grpcServer)
    grpcServer.add_insecure_port(service.addr)
    grpcServer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        grpcServer.stop()