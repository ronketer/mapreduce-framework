[![C++17](https://img.shields.io/badge/C++-17-00599C?style=flat&logo=cplusplus&logoColor=white)](https://en.cppreference.com/)
[![pthreads](https://img.shields.io/badge/pthreads-POSIX-blue)]()
[![Atomic](https://img.shields.io/badge/std::atomic-lock--free-green)]()

# MapReduce Framework — C++17 / pthreads

A multi-threaded MapReduce framework built for the **Operating Systems** course at **Hebrew University**. I implemented the full thread orchestration engine (`MapReduceFramework.cpp`) against a provided API.

→ [See my other projects](https://github.com/ronketer)

## What I implemented

- **Atomic work-stealing** for both map and reduce phases — threads grab the next available input/shuffled-group via a shared `std::atomic<uint64_t>` counter, avoiding a thread-pool queue entirely
- **Per-thread intermediate vectors** during map to eliminate contention, then a single-threaded deterministic shuffle (TID 0) that merges all sorted vectors by K2 key equality using only the `<` operator
- **Two-phase barrier synchronization** — all threads rendezvous after sort (before shuffle) and again after shuffle (before reduce), using a provided reusable `Barrier` built on `pthread_cond_broadcast`
- **Fine-grained locking** — three separate mutexes for map output (`emit2`), reduce output (`emit3`), and job-state reads, keeping critical sections minimal
- **Live progress tracking** via `getJobState`, reporting stage and percentage computed from atomic counters without stalling worker threads

## Phase flow

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Map Phase   │     │ Shuffle Phase│     │ Reduce Phase │
│              │     │              │     │              │
│  Thread 0 ──┐│     │              │     │┌── Thread 0  │
│  Thread 1 ──┼│────▶│   TID 0      │────▶│├── Thread 1  │
│  Thread N ──┘│     │  (single)    │     │└── Thread N  │
│              │     │              │     │              │
│ atomic work- │     │   barrier    │     │ atomic work- │
│   steal      │     │    sync      │     │   steal      │
└──────────────┘     └──────────────┘     └──────────────┘
```

```
All threads        TID 0 only         All threads
──────────         ──────────         ──────────
  MAP (atomic)  →  SORT + barrier  →  SHUFFLE  →  barrier  →  REDUCE (atomic)
```

## Stack

C++17 · pthreads · `std::atomic` · `pthread_mutex_t` · `pthread_cond_t`

## Build

```bash
make            # → libMapReduceFramework.a
make clean
```

Requires g++ (C++17) + pthreads. On Windows use MSYS2/MinGW-w64 or WSL.

## API

Defined in the provided `MapReduceFramework.h`:

```cpp
JobHandle startMapReduceJob(const MapReduceClient&, const InputVec&, OutputVec&, int multiThreadLevel);
void waitForJob(JobHandle job);
void getJobState(JobHandle job, JobState* state);   // stage ∈ {UNDEFINED, MAP, SHUFFLE, REDUCE}, percentage ∈ [0,100]
void closeJobHandle(JobHandle job);
```

Implement `MapReduceClient::map()` and `MapReduceClient::reduce()`, emit pairs via `emit2`/`emit3`. Link with `-L. -lMapReduceFramework -pthread`.
