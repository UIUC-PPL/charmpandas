#include "server.hpp"
#include "converse.h"
#include "conv-ccs.h"

#include "server.decl.h"


Main::Main(CkArgMsg* msg) 
{
    partition_ptr = &partition;
    register_handlers();
    agg_proxy = CProxy_Aggregator::ckNew(thisProxy);
#ifndef NDEBUG
    CkPrintf("Initialization done\n");
#endif
}

Main::Main(CkMigrateMessage *m) : CBase_Main(m)
{
    partition_ptr = &partition;
    register_handlers();
}

void Main::pup(PUP::er &p)
{
    p | partition;
}

void Main::register_handlers()
{
    CcsRegisterHandler("connect", (CmiHandler) Server::connection_handler);
    CcsRegisterHandler("disconnect", (CmiHandler) Server::disconnection_handler);
    CcsRegisterHandler("sync", (CmiHandler) Server::sync_handler);
    CcsRegisterHandler("async", (CmiHandler) Server::async_handler);
    CcsRegisterHandler("async_group", (CmiHandler) Server::async_group_handler);
    CcsRegisterHandler("rescale", (CmiHandler) Server::rescale_handler);
}

void Main::init_done()
{
    partition_ptr->poll();
    agg_proxy.poll();    
    CcsSendDelayedReply(creation_reply, 0, NULL);
}

#include "server.def.h"
