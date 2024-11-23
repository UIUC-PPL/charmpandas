#include "server.hpp"
#include "converse.h"
#include "conv-ccs.h"

#include "server.decl.h"


class Main : public CBase_Main
{
public:
    CProxy_Partition partition;

    Main(CkArgMsg* msg) 
    {
        partition_ptr = &partition;
        register_handlers();
        agg_proxy = CProxy_Aggregator::ckNew();
#ifndef NDEBUG
        CkPrintf("Initialization done\n");
#endif
    }

    Main(CkMigrateMessage *m) : CBase_Main(m)
    {
        partition_ptr = &partition;
        register_handlers();
    }

    void pup(PUP::er &p)
    {
        p | partition;
    }

    void register_handlers()
    {
        CcsRegisterHandler("connect", (CmiHandler) Server::connection_handler);
        CcsRegisterHandler("disconnect", (CmiHandler) Server::disconnection_handler);
        CcsRegisterHandler("sync", (CmiHandler) Server::sync_handler);
        CcsRegisterHandler("async", (CmiHandler) Server::async_handler);
        CcsRegisterHandler("rescale", (CmiHandler) Server::rescale_handler);
    }
};

#include "server.def.h"
