#include <stack>
#include <cinttypes>
#include <variant>
#include <queue>
#include <unordered_map>
#include "partition.hpp"

std::unordered_map<uint8_t, CProxy_Partition> partitions;
std::stack<uint8_t> client_ids;

CProxy_Aggregator agg_proxy;

extern std::unordered_map<int, CcsDelayedReply> fetch_reply;


class Server
{
public:
    static void initialize()
    {
        for (int16_t i = 255; i >= 0; i--)
            client_ids.push((uint8_t) i);
    }

    inline static void insert(uint8_t client, CProxy_Partition partition)
    {
#ifndef NDEBUG
        CkPrintf("Created partition %" PRIu8 " on server\n", client);
#endif
        partitions.emplace(client, partition);
    }

    inline static void remove(uint8_t client)
    {
        partitions.erase(client);
#ifndef NDEBUG
        CkPrintf("Deleted stencil %" PRIu8 " on server\n", client);
#endif
    }

    inline static uint8_t get_client_id()
    {
        if (client_ids.empty())
            CmiAbort("Too many clients connected to the server");
        uint8_t client_id = client_ids.top();
        client_ids.pop();
        return client_id;
    }

    static CProxy_Partition* lookup(uint8_t client)
    {
        auto find = partitions.find(client);
        if (find == std::end(partitions))
        {
#ifndef NDEBUG
            CkPrintf("Stencil %" PRIu8 " not found\n", client);
            CkPrintf("Active stencils: ");
            for (auto it: partitions)
                CkPrintf("%" PRIu8 ", ", it.first);
            CkPrintf("\n");
#endif
            return nullptr;
        }
        return &(find->second);
    }

    static void connection_handler(char* msg)
    {
        uint8_t client_id = get_client_id();
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        int odf = extract<int>(cmd);
        CProxy_Partition partition = create_partition(client_id, odf);
        partition.start();
        CcsSendReply(1, (void*) &client_id);
    }

    static void disconnection_handler(char* msg)
    {
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        uint8_t client_id = extract<uint8_t>(cmd);
        client_ids.push(client_id);
#ifndef NDEBUG
        CkPrintf("Disconnected %" PRIu8 " from server\n", client_id);
#endif
    }

    static void async_handler(char* msg)
    {
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        uint8_t client = extract<uint8_t>(cmd);
        int epoch = extract<int>(cmd);
        int size = extract<int>(cmd);
        CProxy_Partition* partition = lookup(client);
        partition->receive_command(epoch, size, cmd);
    }

    static void sync_handler(char* msg)
    {
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        uint8_t client = extract<uint8_t>(cmd);
        int epoch = extract<int>(cmd);
        int size = extract<int>(cmd);
        CProxy_Partition* partition = lookup(client);
        fetch_reply[epoch] = CcsDelayReply();
        partition->receive_command(epoch, size, cmd);
    }

    inline static void exit_server(char* msg)
    {
        CkExit();
    }

    static CProxy_Partition create_partition(uint8_t client, int odf)
    {
    
        uint32_t total_chares = odf * CkNumPes();

#ifndef NDEBUG
        CkPrintf("Creating partition %" PRIu8 " of size %i\n", 
                client, total_chares);
#endif

        CProxy_Partition new_partition = CProxy_Partition::ckNew(total_chares, agg_proxy, total_chares);
        insert(client, new_partition);
        return new_partition;
    }
};

