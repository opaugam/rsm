## RSM (Replicated State Machines)

### Overview

[**Rust**](https://www.rust-lang.org) crate providing basic blocks for building distributed
systems. It features low level primitives built on atomics as well as higher level constructs such
as a [**Raft**](https://raft.github.io/) toolkit.

The intent is to be able to quickly incorporate non-trivial functionalities in your code, for
instance being able to elect a leader or synchronize a proper draining sequence between multiple
threads. There are two catches: a) the crate does not implement network I/O and you are responsible
for using whatever you prefer, b) it uses half-duplex communication, e.g message passing without
direct acknowledgement.

### Features

 * Fast user-space synchronization primitives built on atomics: lock, read-write lock, gate, once,
   semaphore, events and more!
 * Basic finite state-machine automata, great for building actor systems!
 * Fast [**Raft**](https://raft.github.io/) protocol 100% implemented as an automaton.

Please note this is still a *work in progress* I am working on regularly. It is a great educational
resource for whoever wants to learn how things like locks or finite state-machine work. It is not yet
published on https://crates.io/ but will be soon.

### Example

You often want to experiment when building your next distributed system and want to concentrate on what
matter most. You can efficiently use [**Python**](https://www.python.org/) as a front-end, for instance
to enable [**gRPC**](https://grpc.io/) I/O between your nodes, processing configuration files, etc!

The aptly named [grpc](examples/grpc) example provides the protobuf API definition to encapsulate messages,
a python front-end buffering over unix pipes and sub-processing a rust executable. Try it (assuming you both
have python and grpc installed):

```
$ cargo build --bin grpc
$ cd examples/grpc
$ mkdir api
$ python -m grpc_tools.protoc -I. --python_out=./api --grpc_python_out=./api api.proto
$ python peer.py --id 0
```

Et voila, a raft peer is up and running ready to communicate to form a cluster!

### Support

Contact opaugam@gmail.com for more information about this project.
