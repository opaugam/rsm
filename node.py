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

"""
Sample python wrapper managing a thin gRPC networking layer over our raft state machine. The
interesting part is that this model is stricly half-duplex and we should be able to lose
either outgoing or incoming messages.
"""

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='node.py', prefix_chars='-')
    parser.add_argument('--port', type=int, default=9000, help='grpc TCP port')
    parser.add_argument('--id', type=int, required=True, help='local peer id')
    args = parser.parse_args()

    def _read(pid):

        """
            Background thread piping STDOUT from the rust subprocess. Each line is parsed
            and translated to a gRPC call to the specified target host.
        """

        for line in iter(pid.stdout.readline,''):
            line = line[:-1]
            try:

                #
                # - parse the outgoing request
                # - peek at the destination
                # - open a channel / invoke the gRPC call
                #
                # @todo maintain a LRU cache of channels
                #
                js = json.loads(line)
                channel = grpc.insecure_channel(js['dst']) 
                stub = api_pb2_grpc.NodeStub(channel)
                stub.process(api_pb2.Packet(json=line))
        
            except Exception as failure:

                #
                # - we failed to send the RPC
                # - this is fine: the raft state machine will catch it later
                #   upon a timeout
                #
                pass

    try:

        #
        # - invoke our rust subprocess running the raft state machine
        # - make sure to pipe STDIN/STDOUT
        # - spawn the STDOUT thread (e.g outgoing traffic)
        #
        snippet = './target/debug/node --id %d --host %s' % (args.id, '127.0.0.1:%d' % args.port)
        pid = Popen(snippet.split(' '), stdin=PIPE, stdout=PIPE, env={'RUST_BACKTRACE': 'full'})
        tid = Thread(target=_read, args=(pid,))
        tid.daemon = True
        tid.start()

        #
        # - run a local gRPC endpoint
        # - incoming gRPC calls are acknowledged and the corresponding payload piped to the
        #   rust subprcess with its STDIN
        #
        class _Wrapper(api_pb2_grpc.NodeServicer):

            def process(self, packet, context):
                pid.stdin.write('%s\n' % packet.json)
                return api_pb2.Empty()

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        api_pb2_grpc.add_NodeServicer_to_server(_Wrapper(), server)
        server.add_insecure_port('[::]:%d' % args.port)
        server.start()
    
        try:

            #
            # - idle poll loop waiting on the rust subprocess
            # - upon a CTRL-C the subprocess should drain gracefully and then exit
            #
            while pid.poll() is None:
                time.sleep(1.0)
       
        except KeyboardInterrupt:
            server.stop(0)

    except subprocess.CalledProcessError as failure:
        pass
    