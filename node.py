import argparse
import json
import grpc
import subprocess
import sys
import time

from concurrent import futures
from subprocess import Popen, PIPE, STDOUT
from threading import Thread

sys.path.append('grpc')

import api_pb2
import api_pb2_grpc


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='node.py', prefix_chars='-')
    parser.add_argument('--port', type=int, default=9000, help='grpc TCP port')
    parser.add_argument('--id', type=int, required=True, help='local id')
    parser.add_argument('--seeds', type=str, help="[host:port,]*")
    args = parser.parse_args()

    def _read(pid):

        for line in iter(pid.stdout.readline,''):
            line = line[:-1]
            print('grpc out -> sending %dB "%s"' % (len(line), line))
            try:

                js = json.loads(line)
                channel = grpc.insecure_channel(js['dst']) 
                stub = api_pb2_grpc.NodeStub(channel)
                stub.process(api_pb2.Packet(json=line))
        
            except Exception as failure:
                print 'failed to send %s (%s)' % (line, failure)

    try:

        snippet = './target/release/node --id %d --host %s%s' % (args.id, '127.0.0.1:%d' % args.port, ' --seeds %s' % args.seeds if args.seeds else '')
        pid = Popen(snippet.split(' '), stdin=PIPE, stdout=PIPE)
        tid = Thread(target=_read, args=(pid,))
        tid.daemon = True
        tid.start()

        class _Wrapper(api_pb2_grpc.NodeServicer):

            def process(self, packet, context):
                pid.stdin.write('%s\n' % packet.json)
                return api_pb2.Empty()

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        api_pb2_grpc.add_NodeServicer_to_server(_Wrapper(), server)
        server.add_insecure_port('[::]:%d' % args.port)
        server.start()
    
        try:
            while pid.poll() is None:
                time.sleep(1.0)
       
        except KeyboardInterrupt:
            server.stop(0)

    except subprocess.CalledProcessError as failure:
        pass
    