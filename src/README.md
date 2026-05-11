Shazad Ramzi
1865426

## Submission Contents  
- **Makefile**  
  - Builds `myclient` and `myserver` into `bin/`.  
- **src/**  
  - `myclient.c` – Client: fragments file, sends UDP packets with reliability.  
  - `myserver.c` – Server: simulates drops, reorders packets, reassembles file.  
  - (Any helper `.c`/`.h` files if used.)  
- **bin/**  
  - `myclient` – Client executable.  
  - `myserver` – Server executable.  
- **doc/README.md**  
  - Usage instructions, protocol design, test cases, known issues.  
- **servaddr.conf**  
  - Example server list (`IP port`, supports comments).

---

## Build Instructions  
From the project root, run:  
```bash
make