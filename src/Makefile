CHARMDIR=/home/adityapb1546/charm/charm
CHARMC=$(CHARMDIR)/netlrts-linux-x86_64/bin/charmc
CHARM_INC=-I$(CHARMDIR)/include
OPTS=-c++-option -std=c++17 -O3 -g -ldl -DNDEBUG

all: server.out

.PHONY: clean server.out

server.decl.h: server.ci server.hpp
	$(CHARMC) -E -module CommonLBs server.ci

server.def.h: server.ci server.hpp

messaging.decl.h: messaging.ci messaging.hpp
	$(CHARMC) -E -module CommonLBs messaging.ci

messaging.def.h: messaging.ci messaging.hpp

partition.decl.h: partition.ci partition.hpp
	$(CHARMC) -E -module CommonLBs partition.ci

partition.def.h: partition.ci partition.hpp

reduction.decl.h: reduction.ci reduction.hpp
	$(CHARMC) -E -module CommonLBs reduction.ci

reduction.def.h: reduction.ci reduction.hpp

server.o: server.cpp server.decl.h server.def.h server.hpp \
	partition.decl.h partition.def.h reduction.decl.h reduction.def.h messaging.decl.h messaging.def.h \
	partition.hpp utils.hpp messaging.hpp serialize.hpp reduction.hpp
	$(CHARMC) `pkg-config --cflags --libs arrow` -module CommonLBs -c $< $(OPTS)

partition.o: partition.cpp partition.decl.h partition.def.h reduction.decl.h reduction.def.h \
	messaging.decl.h messaging.def.h partition.hpp utils.hpp messaging.hpp serialize.hpp reduction.hpp
	$(CHARMC) `pkg-config --cflags --libs arrow` -module CommonLBs -c $< $(OPTS)

server.out: partition.o server.o
	$(CHARMC) `pkg-config --cflags --libs arrow` -g -lparquet -larrow_acero -language charm++ -module CommonLBs -o $@ partition.o server.o

run-server: server.out
	./charmrun +p4 ./server.out ++server ++server-port 10000

clean:
	rm *.decl.h *.def.h *.out *.o charmrun