# UDP Network File Replication System

A fault-tolerant client-server file replication application built in C using UDP sockets. The client replicates a file to multiple servers simultaneously using concurrent threads, implementing a custom reliability protocol to handle packet loss and reordering over UDP.

## Features

- Custom reliability protocol — sliding window transmission with Go-Back-N style retransmission
- Concurrent multi-server replication — one thread per server for simultaneous file transfer
- Packet loss handling — automatic retransmission up to 5 times before giving up
- Packet reordering — correct file reassembly guaranteed regardless of packet arrival order
- File integrity validation — output file on server is identical to original input file
- Structured logging — RFC 3339 timestamped CSV logs tracking DATA packets, ACKs, and DROP events
- Configurable drop rate — server supports simulated packet drops for reliability testing
- Error handling — detects server downtime, max retransmission limits, and socket errors gracefully

## Technologies

- C programming language
- UDP sockets
- POSIX threads (pthreads)
- Linux networking stack

## How to Build

```bash
make
```

Executables are placed in the `bin/` directory.

## How to Run

**Start servers:**
```bash
./bin/myserver <port> <drop_percentage> <root_folder_path>
```

**Start client:**
```bash
./bin/myclient <num_servers> <server_config_file> <mss> <window_size> <input_file> <output_file>
```

**Example — 3 servers, 512 byte MSS, window size 10:**
```bash
./bin/myserver 9090 0 server0/
./bin/myserver 9091 0 server1/
./bin/myserver 9092 0 server2/
./bin/myclient 3 servers.conf 512 10 input/testfile output/result
```

**Server config file format:**
```bash
# IP address port
10.1.0.2 9090
10.1.0.3 9091
10.1.0.4 9092
```
## Logging Format
Client and server produce structured CSV logs to stdout:
```bash
# time, lport, rip, rport, DATA or ACK, pktsn, basesn, nextsn, basesn+winsz
2019-02-27T03:19:44.852Z, 9000, 10.1.0.2, 9090, DATA, 7, 5, 8, 15
2019-02-27T03:19:44.852Z, 9000, 10.1.0.2, 9090, ACK, 4, 5, 8, 15
```

## Key Design Decisions

- Sliding window protocol — allows up to `winsz` unacknowledged packets in flight for throughput efficiency
- Per-server threads — client sends concurrently to all replica servers without blocking
- Retransmission logic — packets retransmitted up to 5 times on timeout before failure
- Server-side locking — prevents multiple clients from writing to the same output file simultaneously
- Path handling — server saves files relative to root folder path regardless of absolute paths from client

## Project Structure

```bash
├── Makefile
├── README
├── src/          # C source files
├── bin/          # Compiled executables
└── doc/          # Architecture documentation and test cases
```
