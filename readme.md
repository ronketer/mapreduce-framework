# MapReduce — High-Performance Multithreaded C++ Framework

A lightweight, high-performance MapReduce framework written in **C++17** using **POSIX threads (pthreads)**. This project demonstrates sophisticated thread orchestration, synchronization primitives, and deterministic data processing under high computational load.



## 🛠️ Technical Highlights

* **Thread Orchestration:** Designed a high-performance framework that orchestrates the **MAP → SORT → SHUFFLE → REDUCE** pipeline across N concurrent worker threads. 
* **Advanced Synchronization:** Secured shared data integrity via **mutexes**, **atomic counters**, and a custom reusable **Barrier** to coordinate phases across threads. 
* **Deterministic Concurrency:** Architected a centralized shuffle phase (managed by TID 0) to ensure deterministic intermediate data organization before reduction. 
* **Real-Time State Monitoring:** Implemented a thread-safe progress reporting system (`getJobState`) providing atomic updates on stage transitions and completion percentages.
* **Stress-Tested Reliability:** Developed a rigorous testing strategy to identify and mitigate race conditions and memory leaks, ensuring stable execution under peak load. 

## ⚙️ How It Works

The framework implements a reusable **Barrier** to synchronize threads between distinct execution stages:
1. **Map Stage:** Threads process input pairs and store results in per-thread intermediate vectors.
2. **Sort Stage:** Each thread performs a deterministic sort on its local intermediate data.
3. **Shuffle Stage:** A centralized coordinator reorganizes intermediate data into grouped keys for the reduction phase.
4. **Reduce Stage:** Worker threads process grouped keys to produce the final output vector.

## 📋 Public API

The framework exposes a clean, static library interface (`libMapReduceFramework.a`):

| Function | Description |
| :--- | :--- |
| `startMapReduceJob` | Orchestrates and launches Map and Reduce stages across N worker threads.  |
| `waitForJob` | Blocks until the specific job handle reaches completion. |
| `getJobState` | Retrieves atomic updates on the current stage and progress percentage. |
| `closeJobHandle` | Safely deallocates resources associated with a finished job. |

## 🏗️ Build & Implementation

### Prerequisites
* **Compiler:** g++ with C++17 support.
* **Concurrency:** pthreads (Standard on Linux/macOS; use MSYS2/WSL for Windows).

### Build Instructions
```bash
# Compile and archive the static library
make

# Clean build artifacts
make clean
