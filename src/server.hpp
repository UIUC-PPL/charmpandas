#ifndef SERVER_H
#define SERVER_H

#include <stack>
#include <cinttypes>
#include <variant>
#include <queue>
#include <unordered_map>
#include "conv-ccs.h"
#include "manager.h"
#include "utils.hpp"
#include "partition.hpp"

CProxy_Partition* partition_ptr;

CProxy_Aggregator agg_proxy;

std::unordered_map<int, CcsDelayedReply> fetch_reply;
CcsDelayedReply creation_reply;


class Server
{
public:

    static void rescale_handler(char* msg)
    {
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        int epoch = extract<int>(cmd);
        int new_procs = extract<int>(cmd);
        int size = extract<int>(cmd);
        printf("New = %i, Size = %i\n", new_procs, size);
        char* pass_msg = (char*) malloc(CmiMsgHeaderSizeBytes + CkNumPes() * sizeof(char) + sizeof(int) + sizeof(char));
        char* bitmap = pass_msg + CmiMsgHeaderSizeBytes;
        for (int i = 0; i < CkNumPes(); i++)
        {
            if (i < new_procs)
                bitmap[i] = 1;
            else
                bitmap[i] = 0;
        }
        memcpy(&bitmap[CkNumPes()], &new_procs, sizeof(int));
        bitmap[CkNumPes()+sizeof(int)] = '\0';
        rescale(pass_msg);
        CkPrintf("Rescale epoch = %i\n", epoch);
        //main_proxy.ckLocal()->creation_reply = CcsDelayReply();
        partition_ptr->receive_command(epoch, size, cmd);
        free(pass_msg);
    }

    static void connection_handler(char* msg)
    {
        bool reply = true;
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        int odf = extract<int>(cmd);
        int lb_period = extract<int>(cmd);
        creation_reply = CcsDelayReply();
        create_partition(odf, lb_period);
    }

    static void disconnection_handler(char* msg)
    {
        CkExit();
    }

    static void async_handler(char* msg)
    {
        //CkPrintf("Async handler called\n");
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        int epoch = extract<int>(cmd);
        int size = extract<int>(cmd);
        CkPrintf("Async handler epoch = %i\n", epoch);
        partition_ptr->receive_command(epoch, size, cmd);
    }

    static void async_group_handler(char* msg)
    {
        //CkPrintf("Async handler called\n");
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        int epoch = extract<int>(cmd);
        int size = extract<int>(cmd);
        CkPrintf("Async handler epoch = %i\n", epoch);
        agg_proxy.receive_command(epoch, size, cmd);
    }

    static void sync_handler(char* msg)
    {
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        int epoch = extract<int>(cmd);
        int size = extract<int>(cmd);
        fetch_reply[epoch] = CcsDelayReply();
        partition_ptr->receive_command(epoch, size, cmd);
    }

    static void create_partition(int odf, int lb_period)
    {
    
        uint32_t total_chares = odf * CkNumPes();

#ifndef NDEBUG
        CkPrintf("Creating partition of size %i\n", 
                total_chares);
#endif

        *partition_ptr = CProxy_Partition::ckNew(total_chares, lb_period, agg_proxy, total_chares);
    }
};

#endif
