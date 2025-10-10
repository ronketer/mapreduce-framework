CC = g++
CXX = g++
AR = ar
RANLIB = ranlib

LIBSRC = Barrier.cpp MapReduceFramework.cpp

LIBOBJ = $(LIBSRC:.cpp=.o)

INCS = -I.

CFLAGS = -Wall -std=c++17 -pthread -g $(INCS)
CXXFLAGS = -Wall -std=c++17 -pthread -g $(INCS)

LIBRARY = libMapReduceFramework.a

TAR = tar
TARFLAGS = -cvf
TARNAME = ex3.tar
TARSRCS = $(LIBSRC) Makefile README

all: $(LIBRARY)

$(LIBRARY): $(LIBOBJ)
	$(AR) rcs $@ $^
	$(RANLIB) $@

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean up
clean:
	rm -f $(LIBOBJ) $(LIBRARY) *~ *core

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)

.PHONY: all clean tar
