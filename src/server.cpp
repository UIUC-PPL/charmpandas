#include "server.hpp"
#include "converse.h"
#include "conv-ccs.h"

#ifdef USE_GPU
#include <rmm/mr/device/pool_memory_resource.hpp>
#include <rmm/mr/device/cuda_memory_resource.hpp>
#include <rmm/mr/device/per_device_resource.hpp>
#endif

#include "server.decl.h"


Main::Main(CkArgMsg* msg)
{
#ifdef USE_GPU
    // Initialize RMM pool memory resource for GPU allocations
    static auto cuda_mr = std::make_shared<rmm::mr::cuda_memory_resource>();
    static auto pool_mr = std::make_shared<rmm::mr::pool_memory_resource<rmm::mr::cuda_memory_resource>>(cuda_mr.get());
    rmm::mr::set_current_device_resource(pool_mr.get());
#endif

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
