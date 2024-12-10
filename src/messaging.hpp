#ifndef MESSAGING_H
#define MESSAGING_H

#include <vector>
#include <cstring>
#include <arrow/io/file.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/api.h>
#include <arrow/table.h>
#include <arrow/scalar.h>
#include <arrow/compute/api.h>
#include <parquet/arrow/reader.h>
#include "arrow/acero/exec_plan.h"
#include <arrow/acero/options.h>
#include "arrow/compute/expression.h"
#include "types.hpp"
#include "utils.hpp"
#include "operations.hpp"
#include "serialize.hpp"
#include "messaging.decl.h"

#define LOCAL_JOIN_PRIO 2
#define REMOTE_JOIN_PRIO 1
#define REMOTE_DATA_PRIO 0

class BaseTableDataMsg
{
public:
    char* data;
    int epoch;
    int size;

    BaseTableDataMsg(int epoch_, int size_)
        : epoch(epoch_)
        , size(size_)
    {}

    inline TablePtr get_table()
    {
        if (size > 0)
            return deserialize(data, size);
        else
            return nullptr;
    }
};

class JoinShuffleTableMsg : public CMessage_JoinShuffleTableMsg
{
public:
    char* left_data;
    char* right_data;
    int left_size;
    int right_size;

    JoinShuffleTableMsg(int left_size_, int right_size_)
        : left_size(left_size_)
        , right_size(right_size_)
    {}

    inline TablePtr get_left()
    {
        if (left_size > 0)
            return deserialize(left_data, left_size);
        else
            return nullptr;
    }

    inline TablePtr get_right()
    {
        if (right_size > 0)
            return deserialize(right_data, right_size);
        else
            return nullptr;
    }
};

class RemoteTableMsg : public BaseTableDataMsg, public CMessage_RemoteTableMsg
{
public:
    uint8_t dir;

    RemoteTableMsg(int epoch_, int size_, uint8_t dir_)
        : BaseTableDataMsg(epoch_, size_)
        , dir(dir_)
    {}
};


class GatherTableDataMsg : public BaseTableDataMsg, public CMessage_GatherTableDataMsg
{
public:
    int num_partitions;

    GatherTableDataMsg(int epoch_, int size_, int num_partitions_)
        : BaseTableDataMsg(epoch_, size_)
        , num_partitions(num_partitions_)
    {}
};

class RemoteJoinMsg : public BaseTableDataMsg, public CMessage_RemoteJoinMsg
{
public:
    int fwd_count;

    RemoteJoinMsg(int epoch_, int size_, int fwd_count_)
        : BaseTableDataMsg(epoch_, size_)
        , fwd_count(fwd_count_)
    {}

    RemoteJoinMsg* copy()
    {
        RemoteJoinMsg* new_msg = new (size, 8*sizeof(int)) RemoteJoinMsg(epoch, size, fwd_count + 1);
        std::memcpy(new_msg->data, data, size);
        *((int*) CkPriorityPtr(new_msg)) = REMOTE_DATA_PRIO;
        CkSetQueueing(new_msg, CK_QUEUEING_IFIFO);
        CkSetRefNum(new_msg, epoch);
        return new_msg;
    }
};

class LocalJoinMsg : public CMessage_LocalJoinMsg
{
public:
    TablePtr t1;
    TablePtr t2;
    bool is_remote;

    LocalJoinMsg(TablePtr t1_, TablePtr t2_, bool is_remote_)
        : t1(t1_)
        , t2(t2_)
        , is_remote(is_remote_)
    {}
};

#endif