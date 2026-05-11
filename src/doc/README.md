# Reliable UDP File Replication

## 1. Overview
A reliable file‐replication service over UDP.  
- **Client (`myclient`)**: Splits a local file into MSS‐sized packets, then sends them concurrently to multiple servers with a sliding‐window protocol and retransmissions on timeout.  
- **Server (`myserver`)**: Listens on UDP, simulates packet drops, reorders and buffers out‐of‐order packets, and writes the reassembled file under a given root directory. Prevents two clients from writing the same file simultaneously.

## 2. Usage

### Server

- **port**: UDP port to listen on.  
- **drop%**: 0–100% chance to drop any DATA or ACK packet.  
- **root_folder**: Base directory under which incoming files are saved

### Client

- **replication_factor**: Number of servers (1–10).  
- **servaddr.conf**: Lines of `IP port` (ignores `#`).  
- **MSS**: Minimum payload size (≥20 bytes for header).  
- **winsz**: Sliding‐window size.  
- **in_file**: Local file to send.  
- **out_path**: Path (including directories) where each server saves the file (relative to its root).

## 3. Protocol Highlights
- **Packet (20‐byte header + payload)**:
  - `seq_num` (4B), `packet_type` (4B: 1=DATA, 2=ACK, 20=ERR), `file_path_len` (4B), `data_len` (4B), `checksum` (4B).
  - DATA includes `file_path` string + up to `MSS−20` bytes of file data.
- **Selective‐Repeat Window**:
  - Client tracks `base_seq`, `next_seq`, sends up to `winsz` unACKed packets.
  - Retransmit if no ACK within 3 sec (max 5 retries).  
  - If 5 retries fail → exit code 4 (“Reached max re‐transmission limit”).  
  - If no ACK for 30 sec → exit code 3 (“Cannot detect server IP…”).
- **Concurrency**:
  - One thread per server handles send/ACK/retransmit. Main thread exits with the highest‐priority nonzero code.
- **Server Session**:
  - Keyed by client IP:port + `out_path`. First packet creates directories under `<root_folder>/<out_path>` and opens file.  
  - Writes in‐order bytes; buffers out‐of‐order up to 100 000 slots.  
  - Sends ACK for `expected_seq−1` (with drop simulation).  
  - Cleans up sessions idle > 60 sec.  

## 4. Quick Test Cases
1. **Small File, No Loss**:  
   - `drop%=0`, `MSS=512`, `winsz=10`, single server. Expect exact match, no retransmits.
2. **Moderate File, Some Loss**:  
   - `drop%=25`, `MSS=1024`, `winsz=8`, two servers. Expect retransmits logged, final files match.
3. **High Loss, Large File**:  
   - `drop%=50`, `MSS=1024`, `winsz=16`, three servers. Many retransmits; final integrity OK.
4. **Nested Directories**:  
   - `out_path=newdir/subdir/file.txt`, `drop%=0`. Server auto‐creates directories.
5. **Concurrent Write Conflict**:  
   - Two clients use same `out_path` concurrently. Second client receives error and exits code 20.

## 5. File List
- **Makefile**: Builds `myclient` and `myserver` into `bin/`.  
- **servaddr.conf**: Sample server list.  
- **src/**  
  - `myclient.c`: UDP client with sliding window and threading.  
  - `myserver.c`: UDP server with drop simulation and reassembly.  
- **bin/**  
  - `myclient`, `myserver`: Executables.  

## 6. Known Limitations
- Server buffers up to 100 000 out‐of‐order packets (high memory).  
- Single‐threaded receive loop on server may delay under heavy load.  
- No explicit EOF marker—last ACK loss can delay completion.  
- Fixed max packet size: client MSS−20 ≤ 32748 bytes.
