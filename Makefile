CXX = mpic++
CXXFLAGS = -std=c++11 -Wall -Wextra -pedantic-errors -lpthread -lm -lrt -ltbb
CPPFLAGS = -I$(ROOTSYS)/include `root-config --libs --cflags` -I..

socket_s: socket_s.cxx ParallelFileMerger.hxx masterio.hxx
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -o socket_s socket_s.cxx ParallelFileMerger_hxx.so

clean:
	rm -f socket_s *.log ParallelFileMerger_hxx* toto.*
