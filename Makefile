CXX = mpic++
PROG = test
CXXFLAGS = -std=c++1y -Wall -Wextra -pedantic-errors -lpthread -lm -lrt -fPIC -O2 -g #-ltbb
ROOTLIBS = $(shell root-config --libs)
LOCALLIB = -L$(shell pwd)/lib
INCLUDES = -I$(shell root-config --incdir) -I./include
HEADERS = $(shell pwd)/include/ParallelFileMerger.hxx

all: lib $(PROG)

lib: libParallelFileMerger.so

libParallelFileMerger.so: MyDict.cxx
	if [ ! -d lib ]; then mkdir -p lib; fi
	$(CXX) -shared -o lib/$@ -fPIC $(INCLUDES) $^

MyDict.cxx: $(HEADERS) include/Linkdef.h
	rootcint -f $@ -c -p $^

$(PROG): % : %.o libParallelFileMerger.so
	if [ ! -d bin ]; then mkdir -p bin; fi
	$(CXX) -o bin/$@ $< $(ROOTLIBS) $(LOCALLIB) -lParallelFileMerger

%.o: %.cxx
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c -o $@ $<

clean: 
	-rm -rf lib;
	-rm -rf bin;
	-rm MyDict*;
	-rm *.o
