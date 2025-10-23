# GoCask
GoCask is a simple **Go implementation of the disk-based key-value store**.  
It supports **append-only writes**, **tombstone deletes**, and **in-memory indexing**, inspired by the original BitCask design.

---

## Features

- Append-only **write log**
- **In-memory KeyDir** for fast key lookup
- **Delete** support via tombstones
- Recover from existing log files on startup
- Thread-safe with **sync.RWMutex**
- Benchmarked on Windows (amd64) with Go 1.24+

---

## Installation

```bash
git clone https://github.com/iscoreyagain/GoCask.git
cd GoCask
go build ./...
```

---

## Future Improvements
 - Batch fsync
 - 
