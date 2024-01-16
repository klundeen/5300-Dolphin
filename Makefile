CXX = g++
CXXFLAGS = -std=c++11 -Wall -I/usr/local/db6/include
LDFLAGS = -L/usr/local/db6/lib
LDLIBS = -ldb_cxx -lsqlparser

sqlshell: sqlshell.o
	$(CXX) $(LDFLAGS) -o $@ $^ $(LDLIBS)

sqlshell.o: sqlshell.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean:
	rm -f sqlshell.o sqlshell

all: sqlshell
