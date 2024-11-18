CHARMDIR=/home/adityapb1546/charm/charm
CHARMC=$(CHARMDIR)/netlrts-linux-x86_64/bin/charmc
CHARM_INC=-I$(CHARMDIR)/include
OPTS=-c++-option -std=c++17 -O3 -g -ldl -I/u/bhosale/fmt/include -DNDEBUG

all: server.out

.PHONY: clean server.out

server.decl.h: server.ci server.hpp
	$(CHARMC) -E -module CommonLBs server.ci

server.def.h: server.ci server.hpp

partition.decl.h: partition.ci partition.hpp
	$(CHARMC) -E -module CommonLBs partition.ci

partition.def.h: partition.ci partition.hpp

reduction.decl.h: reduction.ci reduction.hpp
	$(CHARMC) -E -module CommonLBs reduction.ci

reduction.def.h: reduction.ci reduction.hpp

server.o: server.cpp server.decl.h server.def.h server.hpp partition.decl.h partition.def.h \
	reduction.decl.h reduction.def.h partition.hpp utils.hpp serialize.hpp reduction.hpp
	$(CHARMC) `pkg-config --cflags --libs arrow` -module CommonLBs -c $< $(OPTS)

server.out: server.o
	$(CHARMC) `pkg-config --cflags --libs arrow` -g -lparquet -larrow_acero -language charm++ -module CommonLBs -o $@ server.o

run-server: server.out
	./charmrun +p4 ./server.out ++server ++server-port 10000

clean:
	rm *.decl.h *.def.h *.out *.o charmrun