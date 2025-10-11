# MapReduce (C++17, pthreads)

A lightweight, multi-threaded MapReduce framework written in C++17. It exposes a small API you can link into your own program, and uses a reusable barrier to coordinate the map/sort/shuffle/reduce phases across threads.

- Concurrency: pthreads + a reusable `Barrier`
- Artifact: static library `libMapReduceFramework.a`
- Stages: MAP → SORT → SHUFFLE → REDUCE, with progress tracking via `getJobState`

## Features

- Multithreaded map and reduce with per-thread intermediate vectors
- Deterministic per-thread sort, followed by centralized shuffle (TID 0)
- Thread-safe `emit2` (map output) and `emit3` (final output)
- Progress/state reporting: `MAP_STAGE`, `SHUFFLE_STAGE`, `REDUCE_STAGE`
- Simple Makefile with `make`, `clean`, `tar`

## Requirements

- g++ with C++17 support
- make
- pthreads (on Linux/macOS it’s built-in; on Windows use MSYS2/MinGW-w64 or WSL)

Windows notes:
- Native MSVC toolchain doesn’t support `-pthread`. Use one of:
  - MSYS2 MinGW-w64 shell (recommended)
  - WSL (Ubuntu) with build-essential
  - Git Bash with mingw64 toolchain installed

## Build

From the repository root:

```powershell
make
```

This compiles sources and archives the static library:

- Output: `libMapReduceFramework.a`

Other targets:

```powershell
make clean    # remove objects and the library
make tar      # create ex3.tar (expects a README file)
```

## Usage (link the library)

Public API (declared in `MapReduceFramework.h`, implemented here):

- `JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& input, OutputVec& output, int multiThreadLevel);`
- `void waitForJob(JobHandle job);`
- `void getJobState(JobHandle job, JobState* state);`  // `stage` in {UNDEFINED_STAGE, MAP_STAGE, SHUFFLE_STAGE, REDUCE_STAGE}, `percentage` in [0,100]
- `void closeJobHandle(JobHandle job);`

Where `InputVec`, `OutputVec`, key/value types, and the `MapReduceClient` interface are defined by `MapReduceFramework.h` (part of the assignment’s API).

### Minimal example

```cpp
// main.cpp
#include "MapReduceFramework.h"
#include <vector>

struct Client : public MapReduceClient {
  void map(const K1* key, const V1* val, void* ctx) const override {
    // emit2(new K2(...), new V2(...), ctx);
  }
  void reduce(const IntermediateVec* pairs, void* ctx) const override {
    // emit3(new K3(...), new V3(...), ctx);
  }
};

int main() {
  Client c;
  InputVec input;   // fill with (K1*, V1*) pairs
  OutputVec output; // will be filled with (K3*, V3*) pairs

  int threads = 4;
  JobHandle job = startMapReduceJob(c, input, output, threads);

  JobState s{};
  do {
    getJobState(job, &s);
    // optionally print s.stage and s.percentage
  } while (s.stage != REDUCE_STAGE);

  waitForJob(job);
  closeJobHandle(job);
}
```

Compile and link against the library in this repo:

```powershell
g++ -std=c++17 -pthread -I. main.cpp -L. -lMapReduceFramework -o example
```

Note: You must manage the lifetime of allocated keys/values as defined by the assignment’s API. This framework deletes intermediate pairs during teardown.

## Repository layout

- `Barrier.h` / `Barrier.cpp` – reusable pthread barrier used between phases
- `MapReduceFramework.cpp` – implementation of the MapReduce thread orchestration and API
- `Makefile` – builds `libMapReduceFramework.a`; targets: `all` (default), `clean`, `tar`

## Troubleshooting

- "undefined reference to `pthread_*'" → Add `-pthread` to both compile and link, or use a toolchain that supports pthreads.
- `make: command not found` → Install build tools (MSYS2/WSL on Windows).
- `ar` / `ranlib` missing → Install binutils for your environment.
- `make tar` complains about missing `README` → This repository uses `README.md`. Either create a plain `README` stub or update `Makefile`’s `TARSRCS` to include `README.md`.

## Acknowledgements

This project was developed as part of an Operating Systems course to practice multithreaded programming and synchronization primitives.
