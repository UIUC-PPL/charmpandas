#CHARMDIR=/home/adityapb1546/charm/charm
CHARMC=$(CHARMDIR)/netlrts-darwin-arm8/bin/charmc
CHARM_INC=-I$(CHARMDIR)/include
OPTS=-c++-option -std=c++20 -O3 -g -ldl -DNDEBUG -I$(VCPKG_DIR)/include -I$(XXHASH_DIR)/include

all: server.out

.PHONY: clean server.out sortinglib

sortinglib:
	$(MAKE) -C sortinglib CHARMC="$(CHARMC)"

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
	$(CHARMC) `pkg-config --cflags --libs arrow` -module CommonLBs -Isortinglib -c $< $(OPTS)

partition.o: partition.cpp partition.decl.h partition.def.h reduction.decl.h reduction.def.h \
	messaging.decl.h messaging.def.h partition.hpp utils.hpp messaging.hpp serialize.hpp reduction.hpp \
	sortinglib
	$(CHARMC) `pkg-config --cflags --libs arrow` -module CommonLBs -Isortinglib -c $< $(OPTS)

server.out: partition.o server.o sortinglib
	$(CHARMC) `pkg-config --cflags --libs arrow` -L$(XXHASH_DIR)/lib -Lsortinglib -g -lxxhash -lparquet -larrow_acero -larrow_compute -language charm++ -module CommonLBs -module HistSort -module completion -o $@ partition.o server.o

run-server: server.out
	./charmrun +p4 ./server.out ++server ++server-port 10000

clean:
	rm *.decl.h *.def.h *.out *.o charmrun
