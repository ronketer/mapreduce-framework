# MapReduce

This project, developed as part of an Operating Systems course, implements a multithreaded MapReduce framework in C++. It uses a `Barrier` from the `Barrier.cpp` to synchronize threads during different stages of the MapReduce process, which is implemented in MapReduceFramework.cpp.

A Makefile is included for easy compilation for easy compilation.

## Building

run the command `make`.

This will compile the source files and produce the static library `libMapReduceFramework.a`.

## Files

- Barrier.cpp: Implementation of a reusable barrier using pthreads.
- Barrier.h: Header file defining the Barrier class.
- MapReduceFramework.cpp: Core logic for multithreaded MapReduce processing.
- Makefile: Build script for compiling and archiving the library.

## Usage

After building, link `libMapReduceFramework.a` with your own C++ program that includes the MapReduce logic.
