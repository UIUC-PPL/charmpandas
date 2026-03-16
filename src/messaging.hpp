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

class RedistTableMsg : public CMessage_RedistTableMsg
{
public:
    char* data;
    int* sizes;
    int num_tables;
    int total_size;

    RedistTableMsg(std::vector<BufferPtr> &buffers, int total_size_)
        : total_size(total_size_)
    {
        num_tables = buffers.size();
        int offset = 0;
        for (int i = 0; i < buffers.size(); i++)
        {
            if (buffers[i] == nullptr)
            {
                sizes[i] = 0;
            }
            else
            {
                sizes[i] = buffers[i]->size();
                std::memcpy(data + offset, buffers[i]->data(), sizes[i]);
            }
            offset += sizes[i];
        }
    }

    std::vector<TablePtr> get_tables()
    {
        std::vector<TablePtr> tables;
        int offset = 0;
        for (int i = 0; i < num_tables; i++)
        {
            if (sizes[i] == 0)
            {
                tables.push_back(nullptr);
            }
            else
            {
                tables.push_back(deserialize(data + offset, sizes[i]));
                offset += sizes[i];
            }
        }
        return tables;
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
    int chareIdx;
    int num_partitions;

    GatherTableDataMsg(int epoch_, int size_, int chareIdx_, int num_partitions_)
        : BaseTableDataMsg(epoch_, size_)
        , chareIdx(chareIdx_)
        , num_partitions(num_partitions_)
    {}
};

class SortTableMsg : public BaseTableDataMsg, public CMessage_SortTableMsg
{
public:
    SortTableMsg(int epoch_, int size_)
        : BaseTableDataMsg(epoch_, size_)
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

// ============================================================================
// GPU-aware messages for RDMA transfer
// Always declared (since .ci declares them unconditionally),
// but get_table() only available under USE_GPU.
// ============================================================================

class GpuTableMsg : public CMessage_GpuTableMsg
{
public:
    char* metadata;      // host: cudf packed metadata + column names
    char* gpu_data;      // device: contiguous table data (GPU-Direct RDMA)
    int metadata_size;
    int gpu_data_size;
    int epoch;

    GpuTableMsg(int epoch_, int metadata_size_, int gpu_data_size_)
        : epoch(epoch_)
        , metadata_size(metadata_size_)
        , gpu_data_size(gpu_data_size_)
    {}

#ifdef USE_GPU
    TablePtr get_table()
    {
        if (gpu_data_size > 0)
            return gpu_unpack(
                reinterpret_cast<const uint8_t*>(metadata),
                reinterpret_cast<const uint8_t*>(gpu_data),
                gpu_data_size);
        else
            return nullptr;
    }
#endif
};

class GpuGatherMsg : public CMessage_GpuGatherMsg
{
public:
    char* metadata;
    char* gpu_data;
    int metadata_size;
    int gpu_data_size;
    int epoch;
    int chareIdx;
    int num_partitions;

    GpuGatherMsg(int epoch_, int metadata_size_, int gpu_data_size_,
        int chareIdx_, int num_partitions_)
        : epoch(epoch_)
        , metadata_size(metadata_size_)
        , gpu_data_size(gpu_data_size_)
        , chareIdx(chareIdx_)
        , num_partitions(num_partitions_)
    {}

#ifdef USE_GPU
    TablePtr get_table()
    {
        if (gpu_data_size > 0)
            return gpu_unpack(
                reinterpret_cast<const uint8_t*>(metadata),
                reinterpret_cast<const uint8_t*>(gpu_data),
                gpu_data_size);
        else
            return nullptr;
    }
#endif
};

class GpuRedistMsg : public CMessage_GpuRedistMsg
{
public:
    char* metadata;      // host: per-table cudf metadata + column names
    int* offsets;        // host: byte offset of each table in gpu_data
    char* gpu_data;      // device: concatenated table data (GPU-Direct RDMA)
    int num_tables;
    int total_metadata_size;
    int total_gpu_size;

    GpuRedistMsg(int num_tables_, int total_metadata_size_, int total_gpu_size_)
        : num_tables(num_tables_)
        , total_metadata_size(total_metadata_size_)
        , total_gpu_size(total_gpu_size_)
    {}

#ifdef USE_GPU
    std::vector<TablePtr> get_tables()
    {
        std::vector<TablePtr> result;
        int meta_offset = 0;
        for (int i = 0; i < num_tables; i++)
        {
            int gpu_offset = offsets[i];
            int gpu_size = (i + 1 < num_tables)
                ? offsets[i + 1] - gpu_offset
                : total_gpu_size - gpu_offset;

            if (gpu_size > 0)
            {
                result.push_back(gpu_unpack(
                    reinterpret_cast<const uint8_t*>(metadata + meta_offset),
                    reinterpret_cast<const uint8_t*>(gpu_data + gpu_offset),
                    gpu_size));
            }
            else
            {
                result.push_back(nullptr);
            }

            // Advance past this table's host metadata
            uint32_t meta_size = *reinterpret_cast<const uint32_t*>(
                metadata + meta_offset);
            meta_offset += sizeof(uint32_t) + meta_size;
            uint32_t names_size = *reinterpret_cast<const uint32_t*>(
                metadata + meta_offset);
            meta_offset += sizeof(uint32_t) + names_size;
        }
        return result;
    }
#endif
};

#endif