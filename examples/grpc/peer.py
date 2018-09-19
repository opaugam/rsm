import argparse
import fcntl
import grpc
import os
import select
import subprocess
import sys
import time
import yaml

from concurrent import futures
from os import path
from subprocess import Popen, PIPE, STDOUT
from threading import Thread

pwd = path.dirname(path.abspath(__file__))
sys.path.append('%s/api' % pwd)

import api_pb2
import api_pb2_grpc

"""
CLI gRPC front-end for a RSM automaton using STDIN/STDOUT pipes as line buffers. The automaton
itself is an executable invoked using subprocess. This pattern allows for building quickly the
network I/O frontend outside of the Rust code. It is also convenient to parse configuration
files, etc.
"""

def _to_varint(n):
    buf = b''
    while 1:
        val = n & 0x7f
        n >>= 7
        if n:
            buf += chr(val | 0x80)
        else:
            buf += chr(val)
            break
    return buf

def _from_varint(buf):
    n = 0
    out = 0
    shift = 0
    while 1:
        if n >= len(buf):
            return 0, 0
        val = ord(buf[n])
        out |= (val & 0x7f) << shift       
        shift += 7
        n += 1
        if not (val & 0x80):
            break

    return out, n

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='node.py', prefix_chars='-')
    parser.add_argument('--id', type=int, required=True, help='peer ID')
    args = parser.parse_args()
        
    def _read(pid):

        off = 0
        chunk = 0
        line = b''
        fd = pid.stdout.fileno()
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        while 1:

            #
            # - select the subprocess STDIN pipe
            # - read whenever ready
            # - accumulate in the line buffer
            #
            select.select([fd], [], [])
            line += pid.stdout.read()
            total = len(line)
            while 1:

                #
                # - try to decode as many chunks as possible from the line buffer
                #   we have
                #
                if off:
                    if total >= off + chunk:

                        #
                        # - we have enough bytes to recover the next chunk
                        # - the fist 32 bytes are the network identifier
                        #
                        payload = line[off: off + chunk]
                        host = payload[:32]
                        try:

                            #
                            # - invoke a process() RPC
                            # @todo use a cache for the channel
                            #
                            channel = grpc.insecure_channel(str(host)) 
                            stub = api_pb2_grpc.NodeStub(channel)
                            stub.process(api_pb2.Packet(raw=payload[32:]))

                        except:
                            pass

                        #
                        # - truncate the line buffer
                        # - reset the counters
                        #
                        line = line[off+chunk:]
                        total -= (off+chunk)
                        chunk = 0
                        off = 0

                    else:
                        break

                else:

                    #
                    # - we are starting to read a new chunk
                    # - first decode the leading varint
                    #
                    chunk, off = _from_varint(line)
                    if not chunk:
                        break

    try:

        #
        # - load the peer list
        # - find our host:port
        #
        try:
            with open('%s/cluster.yml' % pwd, 'rb') as fd:
                peers = yaml.load(fd)
                assert type(peers) is list, 'invalid YAML layout'
                host = peers[args.id]
        except IOError:
            assert 0, 'YAML file not found'
        except yaml.YAMLError:
            assert 0, 'invalid YAML syntax'
        except IndexError:
            assert 0, '%d does not map to any peer' % args.id

        #
        # - extract the port to listen on
        #
        try:
            port = int(host.split(':')[1])
        except:
            assert 0, 'invalid host specification (%s)' % host

        #
        # - invoke our rust subprocess running the raft state machine
        # - make sure to pipe STDIN/STDOUT
        # - spawn the STDOUT thread (e.g outgoing traffic)
        #
        snippet = '%s/../../target/debug/grpc --id %d %s' % (pwd, args.id, ' '.join(peers))
        try:
            pid = Popen(snippet.split(' '), stdin=PIPE, stdout=PIPE, env={'RUST_BACKTRACE': 'full'})
        except OSError:
            assert 0, 'executable not built, run "cargo build --bin grpc" first'

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

                #
                # - data in: pack the byte buffer length as a varint
                # - write both to the subprocess STDIN pipe
                #
                buf = _to_varint(len(packet.raw)) + packet.raw
                pid.stdin.write(buf)
                pid.stdin.flush()
                return api_pb2.Empty()

        try:
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
            api_pb2_grpc.add_NodeServicer_to_server(_Wrapper(), server)
            server.add_insecure_port('[::]:%d' % port)
            server.start()

            #
            # - idle poll loop waiting on the rust subprocess
            # - upon a CTRL-C the subprocess should drain gracefully and then exit
            #
            while pid.poll() is None:
                time.sleep(1.0)
    
        except KeyboardInterrupt:
            pass

        server.stop(0)
        sys.exit(0)

    except Exception as failure:
        print failure
        
    sys.exit(1)