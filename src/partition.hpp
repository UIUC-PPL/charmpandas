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
#include "utils.hpp"
#include "operations.hpp"
#include "reduction.hpp"
#include "serialize.hpp"
#include "partition.decl.h"


#define LOCAL_JOIN_PRIO 2
#define REMOTE_JOIN_PRIO 1
#define REMOTE_DATA_PRIO 0

#define RIGHT 0
#define LEFT 1


// This needs to have client id in the key
std::unordered_map<int, CcsDelayedReply> fetch_reply;


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
        return deserialize(data, size);
    }
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

class JoinTableDataMsg : public BaseTableDataMsg, public CMessage_JoinTableDataMsg
{
public:
    char* left_keys;
    char* right_keys;
    int* lkey_sizes;
    int* rkey_sizes;
    int nkeys;
    int table1_name;
    int result_name;
    int join_type;
    int fwd_count;

    JoinTableDataMsg(int epoch_, int size_, int table1_name_, int result_name_, int join_type_,
        int nkeys_, int fwd_count_)
        : BaseTableDataMsg(epoch_, size_)
        , table1_name(table1_name_)
        , result_name(result_name_)
        , join_type(join_type_)
        , nkeys(nkeys_)
        , fwd_count(fwd_count_)
    {}

    inline std::vector<arrow::FieldRef> get_left_keys()
    {
        std::vector<arrow::FieldRef> keys;
        int offset = 0;
        for (int i = 0; i < nkeys; i++)
        {
            int lkey_size = lkey_sizes[i];
            keys.push_back(arrow::FieldRef(std::string(left_keys + offset, lkey_size)));
            offset += lkey_size;
        }
        return keys;
    }

    inline std::vector<arrow::FieldRef> get_right_keys()
    {
        std::vector<arrow::FieldRef> keys;
        int offset = 0;
        for (int i = 0; i < nkeys; i++)
        {
            int rkey_size = rkey_sizes[i];
            keys.push_back(arrow::FieldRef(std::string(right_keys + offset, rkey_size)));
            offset += rkey_size;
        }
        return keys;
    }

    static void pack_message(JoinTableDataMsg* msg, char* data_, std::vector<arrow::FieldRef> &left_keys_, 
        std::vector<arrow::FieldRef> &right_keys_, int* lkey_sizes_, int* rkey_sizes_)
    {
        if (data_ != nullptr)
            std::memcpy(msg->data, data_, msg->size);
        int left_offset = 0, right_offset = 0;
        std::memcpy(msg->lkey_sizes, lkey_sizes_, msg->nkeys * sizeof(int));
        std::memcpy(msg->rkey_sizes, rkey_sizes_, msg->nkeys * sizeof(int));
        for (int i = 0; i < msg->nkeys; i++)
        {
            std::memcpy(msg->left_keys + left_offset, left_keys_[i].name()->c_str(), lkey_sizes_[i]);
            std::memcpy(msg->right_keys + right_offset, right_keys_[i].name()->c_str(), rkey_sizes_[i]);
            left_offset += lkey_sizes_[i];
            right_offset += rkey_sizes_[i];
        }
    }

    JoinTableDataMsg* copy()
    {
        int total_lsize = 0, total_rsize = 0;
        for (int i = 0; i < nkeys; i++)
        {
            total_lsize += lkey_sizes[i];
            total_rsize += rkey_sizes[i];
        }

        JoinTableDataMsg* msg = new (size, total_lsize, total_rsize, nkeys * sizeof(int), 
            nkeys * sizeof(int)) JoinTableDataMsg(epoch, size, table1_name, result_name, join_type,
            nkeys, fwd_count + 1);

        std::memcpy(msg->data, data, size);
        std::memcpy(msg->left_keys, left_keys, total_lsize);
        std::memcpy(msg->right_keys, right_keys, total_rsize);
        std::memcpy(msg->lkey_sizes, lkey_sizes, nkeys * sizeof(int));
        std::memcpy(msg->rkey_sizes, rkey_sizes, nkeys * sizeof(int));
        return msg;
    }
};

class LocalJoinMsg : CMessage_LocalJoinMsg
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
}


class JoinOptions
{
public:
    int table1, table2;
    int result_name;
    arrow::acero::HashJoinNodeOptions opts;

    JoinOptions(int table1_, int table2_, int result_name_, arrow::acero::HashJoinNodeOptions opts_)
        : table1(table1_)
        , table2(table2_)
        , result_name(result_name_)
        , opts(opts_)
    {}
};

// This should not be a group!
class Aggregator : public CBase_Aggregator
{
    using RemoteBuffer = std::unordered_map<TablePtr, std::pair<bool, int>>;
    using RemoteMsgBuffer = std::unordered_map<TablePtr, RemoteJoinMsg*>;

private:
    CProxy_Partition partition_proxy;
    int num_partitions;

    std::unordered_map<int, int> gather_count;
    std::unordered_map<int, std::vector<GatherTableDataMsg*>> gather_buffer;

    std::vector<int> local_chares;

    // for joins
    int num_local_chares;
    int remote_buffer_limit;
    int next_local_chare;
    int num_sends;
    int num_recvs;
    int num_active_requests;
    int join_count;

    TablePtr local_t1, local_t2;

    JoinOptions* join_opts;
    RemoteBuffer remote_tables;
    RemoteMsgBuffer remote_msgs;

    std::unordered_map<int, TablePtr> result_tables;
    std::unordered_map<int, TablePtr> result_indices;

    int EPOCH;

public:
    Aggregator() {}

    Aggregator(CkMigrateMessage* m) : CBase_Aggregator(m) {}

    void pup(PUP::er &p) {}

    void gather_table(GatherTableDataMsg* msg)
    {
        //TablePtr table = deserialize(data, size);
        auto it = gather_count.find(msg->epoch);
        if (it == gather_count.end())
            gather_count[msg->epoch] = 1;
        else
            gather_count[msg->epoch]++;
        gather_buffer[msg->epoch].push_back(msg);

        if (gather_count[msg->epoch] == msg->num_partitions)
        {
            std::vector<TablePtr> gathered_tables;
            for (int i = 0; i < gather_buffer[msg->epoch].size(); i++)
            {
                GatherTableDataMsg* buff_msg = gather_buffer[msg->epoch][i];
                if (buff_msg->size == 0)
                    continue;
                gathered_tables.push_back(deserialize(buff_msg->data, buff_msg->size));
            }
            //CkPrintf("Received all pieces, size = %i\n", gathered_tables.size());
            // table is gathered, send to server and reply ccs
            auto combined_table = arrow::ConcatenateTables(gathered_tables).ValueOrDie();
            CkPrintf("Gathered table data in aggregator with size = %i\n", combined_table->num_rows());
            BufferPtr out;
            serialize(combined_table, out);
            fetch_callback(msg->epoch, out);

            // FIXME delete saved msgs and combined_msg here
            clear_gather_buffer(msg->epoch);
        }
    }

    void clear_gather_buffer(int epoch)
    {
        gather_count[epoch] = 0;
        GatherTableDataMsg* msg;
        for (int i = 0; i < gather_buffer[epoch].size(); i++)
        {
            msg = gather_buffer[epoch][i];
            delete msg;
        }
        gather_buffer[epoch].clear();
    }

    void fetch_callback(int epoch, BufferPtr &out)
    {
        CkAssert(CkMyPe() == 0);
        CcsSendDelayedReply(fetch_reply[epoch], out->size(), (char*) out->data());
    }

    void start_join()
    {
        for (int i = 0; i < remote_buffer_limit; i++)
        {
            if (i < num_local_chares)
            {
                send_local_data();
                ++num_sends;
            }
        }

        std::vector<TablePtr> local_list1, local_list2;

        for (int i = 0; i < num_local_chares; i++)
        {
            int index = local_chares[i];
            Partition* partition = partition_proxy[index].ckLocal();
            local_list1.push_back(partition->get_table(join_opts->table1, fields));
            local_list2.push_back(partition->get_table(join_opts->table2, fields));
        }

        local_t1 = arrow::ConcatenateTables(local_list1).ValueOrDie();
        local_t2 = arrow::ConcatenateTables(local_list2).ValueOrDie();

        LocalJoinMsg* msg = new (8*sizeof(int)) LocalJoinMsg(local_t1, local_t2, false);
        *((int*) CkPriorityPtr(msg)) = LOCAL_JOIN_PRIO;
        CkSetQueueing(msg, CK_QUEUEING_IFIFO);
        thisProxy[thisIndex].join(msg);
    }

    void check_remote_table(TablePtr &table)
    {
        auto it = remote_tables[table];
        int fwd_count = remote_msgs[table]->fwd_count;
        if (it.second == num_local_chares && (it.first || fwd_count == CkNumPes() - 1))
        {
            remote_tables.erase(table);
            delete remote_msgs[table];
            remote_msgs.erase(table);
            if (join_count != num_local_chares && num_recvs >= remote_buffer_limit)
                thisProxy[(thisIndex - 1) % CkNumPes()].request_join_data();
        }
    }

    TablePtr select_remote()
    {
        for (auto it = remote_tables.begin(); it != remote_tables.end(); ++it)
            if (remote_msgs[it->first]->fwd_count != CkNumPes() - 1)
                return it->first;
        return nullptr;
    }

    void send_local_data()
    {
        int index = local_chares[next_local_chare++];
        Partition* partition = partition_proxy[index].ckLocal();
        BufferPtr out;
        TablePtr table = partition->get_table(join_opts->table2);
        RemoteJoinMsg* msg;
        if (table != nullptr)
        {
            serialize(table, out);
            msg = new (out->size(), 8*sizeof(int)) RemoteJoinMsg(EPOCH, out->size(), 0);
            std::memcpy(msg->data, out->data(), out->size());
        }
        else
        {
            msg = new (0, 8*sizeof(int)) RemoteJoinMsg(EPOCH, 0, 0);
        }

        *((int*) CkPriorityPtr(msg)) = REMOTE_DATA_PRIO;
        CkSetQueueing(msg, CK_QUEUEING_IFIFO);
        CkSetRefNum(msg, EPOCH);
        thisProxy[(thisIndex + 1) % CkNumPes()].receive_remote_indices(msg);
    }

    void request_join_data()
    {
        // forward table from buffer or
        // send local chare data is buffer is empty
        // do local joins during the free time

        if (remote_tables.size() > 0)
        {
            TablePtr remote_table = select_remote();
            if (remote_table != nullptr)
            {
                RemoteJoinMsg* fwd_msg = remote_msgs[remote_table]->copy();
                thisProxy[(thisIndex + 1) % CkNumPes()].receive_remote_indices(fwd_msg);
                
                // set remote status to sent
                remote_tables[remote_table].first = true;
                // check if this remote table is done
                check_remote_table(remote_table);
            }
            else
            {
                if (next_local_chare == num_local_chares)
                    num_active_requests++;
                else
                    send_local_data();
            }
        }
        else
        {
            // send next_local_chare
            send_local_data();
        }
    }

    void process_remote_indices(RemoteJoinMsg* msg)
    {
        // this method should put the remote table
        // in the remote_tables buffer and call
        // entry methods for local joins with the remote data

        // the local joins with remote data should be second highest
        // priority
        if (++num_recvs < num_partitions - num_local_chares)
            thisProxy[thisIndex].listen_remote_table();
        TablePtr table = msg->get_table();
        remote_msgs[table] = msg;
        remote_tables[table] = std::make_pair(false, 0);

        if (num_sends++ < remote_buffer_limit || num_active_requests > 0)
        {
            RemoteJoinMsg* fwd_msg = msg->copy();
            thisProxy[(thisIndex + 1) % CkNumPes()].receive_remote_indices(fwd_msg);
            --num_active_requests;
        }

        for (int i = 0; i < num_local_chares; i++)
        {
            int idx = local_chares[i];
            TablePtr t1 = partition_proxy[idx].ckLocal()->get_table(join_opts->table1);
            // allocate msg and assign priority
            LocalJoinMsg* msg = new (8*sizeof(int)) LocalJoinMsg(t1, table, true);
            *((int*) CkPriorityPtr(msg)) = REMOTE_JOIN_PRIO;
            CkSetQueueing(msg, CK_QUEUEING_IFIFO);
            thisProxy[thisIndex].join(msg);
        }
    }

    void send_table_requests(ChunkedArrayPtr partitions, ChunkedArrayPtr indices, uint8_t dir)
    {
        int last_partition = -1;
        std::vector<int> local_indices;
        for (int i = 0; i < partitions->length(); i++)
        {
            if (partitions->GetScalar(i) != last_partition)
            {
                if (last_partition != -1)
                {
                    ++num_expected_tables;
                    if () // if partition is local
                        // this can be made faster by avoiding local_indices pup
                        partition_proxy[last_partition].request_local_table(local_indices, dir);
                    else
                        partition_proxy[last_partition].request_remote_table(local_indices, thisIndex, dir);
                }
                local_indices.clear();
                last_partition = partitions->GetScalar(i);
            }
            local_indices.push_back(indices->GetScalar(i));
        }
    }

    void fetch_joined_table(TablePtr joined_table)
    {
        // fetch rest of the columns of the joined table
        // first do right, left are local to this PE
        TablePtr right_table = joined_table->SelectColumns(right_keys);
        right_table = right_table->sort_by({{"home_partition_r", "ascending"}});
        send_table_requests(right_table->GetColumnByName("home_partition_r"),
            right_table->GetColumnByName("local_index_r"), RIGHT);

        TablePtr left_table = joined_table->SelectColumns(left_keys);
        left_table = left_table->sort_by({{"home_partition_l", "ascending"}});
        send_table_requests(left_table->GetColumnByName("home_partition_l"),
            left_table->GetColumnByName("local_index_l"), LEFT);
    }

    void complete_join()
    {
        delete join_opts;
        join_opts = nullptr;
        //EPOCH++;

        
    }

    TablePtr local_join(TablePtr &t1, TablePtr &t2, arrow::acero::HashJoinNodeOptions &join_opts)
    {
        // join t1 and t2 and concatenate to result
        arrow::acero::Declaration left{"table_source", arrow::acero::TableSourceNodeOptions(t1)};
        arrow::acero::Declaration right{"table_source", arrow::acero::TableSourceNodeOptions(t2)};

        arrow::acero::Declaration hashjoin{"hashjoin", {std::move(left), std::move(right)}, join_opts};

        // Collect the results
        return arrow::acero::DeclarationToTable(std::move(hashjoin)).ValueOrDie();
    }    

    void join(LocalJoinMsg* msg)
    {
        TablePtr result = local_join(msg->t1, msg->t2, join_opts->opts);
        fetch_joined_table(result);

        auto it = result_indices.find(join_opts->result_name);
        if (it == std::end(result_indices))
            result_indices[join_opts->result_name] = result;
        else
            it->second = arrow::ConcatenateTables({it->second, result}).ValueOrDie();

        for (int i = 0; i < num_local_chares; i++)
        {
            int index = local_chares[i];
            Partition* partition = partition_proxy[index].ckLocal();
            partition->join_count += num_local_chares;
            if (partition->join_count == num_partitions)
            {
                partition->inc_epoch();
                ++join_count;
            }
        }

        if (join_count == num_local_chares)
            completed_requests = true;
        
        if (msg->is_remote)
        {
            remote_tables[msg->t2].second++;
            // check if this remote table is done
            check_remote_table(msg->t2);
        }
    }

    void receive_remote_table(RemoteTableMsg* msg)
    {
        if (completed_requests && --num_expected_tables == 0)
        {
            partition_table(result_tables[join_opts->result_name], join_opts->result_name);
            complete_join();
        }
    }

    void receive_local_table(LocalTableMsg* msg)
    {
        if (completed_requests && --num_expected_tables == 0)
        {
            partition_table(result_tables[join_opts->result_name], join_opts->result_name);
            complete_join();
        }
    }

    void partition_table(TablePtr table, int result_name)
    {
        // partition table into local chares
        int remain_chares = num_local_chares;
        int remain_rows = table.num_rows();
        int offset = 0;
        for (int i = 0; i < num_local_chares; i++)
        {
            int index = local_chares[i];
            Partition* partition = partition_proxy[index].ckLocal();
            int rows_per_chare = remain_rows / remain_chares;
            partition->add_table(result_name, table->Slice(offset, rows_per_chare));
            offset += rows_per_chare;
            remain_rows -= rows_per_chare;
            remain_chares--;
        }
    }
};


class Partition : public CBase_Partition
{
private:
    CProxy_Aggregator agg_proxy;
    int num_partitions;
    int EPOCH;
    int join_count;
    bool local_join_done;
    int lb_period;
    std::unordered_map<int, TablePtr> tables;
    std::unordered_map<int, BufferPtr> tables_serialized;

public:
    Partition_SDAG_CODE

    Partition(int num_partitions_, int lb_period_, CProxy_Aggregator agg_proxy_) 
        : num_partitions(num_partitions_)
        , agg_proxy(agg_proxy_)
        , EPOCH(0)
        , join_count(0)
        , local_join_done(false)
        , lb_period(lb_period_)
    {
        usesAtSync = true;
    }

    Partition(CkMigrateMessage* m) 
        : CBase_Partition(m)
    {
        usesAtSync = true;
        //CkPrintf("Chare %i> Resume polling waiting for epoch = %i\n", thisIndex, EPOCH);
        //thisProxy[thisIndex].poll();
    }

    ~Partition()
    {
        // delete tables?
        tables.clear();
        tables_serialized.clear();
    }

    void pup(PUP::er &p)
    {
        p | agg_proxy;
        p | num_partitions;
        p | EPOCH;
        p | join_count;
        p | local_join_done;
        p | lb_period;
        pup_tables(p);
        if (p.isUnpacking())
        {
            CkPrintf("Chare %i> Resume polling waiting for epoch = %i\n", thisIndex, EPOCH);
            thisProxy[thisIndex].poll();
        }
    }

    void pup_tables(PUP::er &p)
    {
        int num_tables;
        if (!p.isUnpacking())
            num_tables = tables.size();
        p | num_tables;

        std::unordered_map<int, TablePtr>::iterator it;
        if (!p.isUnpacking())
            it = tables.begin();
        for (int i = 0; i < num_tables; i++)
        {
            int table_name, serialized_size;
            if (!p.isUnpacking())
            {
                table_name = it->first;
                auto it_serial = tables_serialized.find(table_name);
                if (it_serial == std::end(tables_serialized))
                    serialize_and_cache(table_name);
                serialized_size = tables_serialized[table_name]->size();
                //CkPrintf("Serialized table %i with size %i\n", table_name, serialized_size);
                p | table_name;
                p | serialized_size;
                p((char*) tables_serialized[table_name]->data(), serialized_size);
                ++it;

                if (p.isPacking())
                    tables_serialized.erase(table_name);
            }
            else
            {
                p | table_name;
                p | serialized_size;
                //CkPrintf("Deserialized table %i with size %i\n", table_name, serialized_size);
                char* buf = new char[serialized_size];
                p(buf, serialized_size);
                tables[table_name] = deserialize(buf, serialized_size);
                // TODO: try to see what happens if I delete the buf
                // Answer: Do not delete buf!
            }
        }
    }

    int64_t calculate_memory_usage()
    {
        int total_size = 0;
        for (auto &it: tables)
        {
            total_size += calculate_memory_usage(it.second);
        }
        return total_size;
    }

    int64_t calculate_memory_usage(TablePtr table)
    {
        int64_t total_size = 0;
    
        for (int i = 0; i < table->num_columns(); ++i) 
        {
            auto column = table->column(i);
            for (int j = 0; j < column->num_chunks(); ++j) 
            {
                auto chunk = column->chunk(j);
                for (const auto& buffer : chunk->data()->buffers) 
                {
                    if (buffer)
                        total_size += buffer->size();
                }
            }
        }
        return total_size;
    }

    void serialize_and_cache(int table_name)
    {
        BufferPtr out;
        serialize(tables[table_name], out);
        tables_serialized[table_name] = out;
    }

    void complete_operation()
    {
        ++EPOCH;
        thisProxy[thisIndex].poll();
    }

    inline void inc_epoch()
    {
        ++EPOCH;
    }

    void ResumeFromSync()
    {
        //CkPrintf("Resume called\n");
        thisProxy[thisIndex].poll();
    }

    TablePtr get_table(int table_name)
    {
        auto it = tables.find(table_name);
        if (it == std::end(tables))
            return nullptr;
        else
            return it->second;
    }

    TablePtr get_table(int table_name, std::vector<string> fields)
    {
        TablePtr table = get_table(table_name);
        return table->SelectColumns(fields);
    }

    inline void add_table(int table_name, TablePtr table)
    {
        tables[table_name] = table;
    }

    void operation_read(char* cmd)
    {
        int table_name = extract<int>(cmd);
        int path_size = extract<int>(cmd);
        std::string file_path(cmd, path_size);
        CkPrintf("[%d] Reading file: %s\n", thisIndex, file_path.c_str());
        read_parquet(table_name, file_path);
        complete_operation();
    }

    void operation_fetch(char* cmd)
    {
        int table_name = extract<int>(cmd);
        auto it = tables.find(table_name);
        GatherTableDataMsg* msg;
        if (it != std::end(tables))
        {
            auto table = tables[table_name];
            BufferPtr out;
            serialize(table, out);
            msg = new (out->size()) GatherTableDataMsg(EPOCH, out->size(), num_partitions);
            std::memcpy(msg->data, out->data(), out->size());
        }
        else
        {
            //CkPrintf("Table not found on chare %i\n", thisIndex);
            msg = new (0) GatherTableDataMsg(EPOCH, 0, num_partitions);
        }
        agg_proxy[0].gather_table(msg);
        complete_operation();
    }

    void operation_join(char* cmd)
    {
        int table1 = extract<int>(cmd);
        int table2 = extract<int>(cmd);
        int result_name = extract<int>(cmd);
        int nkeys = extract<int>(cmd);

        std::vector<arrow::FieldRef> left_keys, right_keys;
        std::vector<int> lkey_sizes, rkey_sizes;
        int total_lsize = 0, total_rsize = 0;
        for (int i = 0; i < nkeys; i++)
        {
            int lkey_size = extract<int>(cmd);
            left_keys.push_back(arrow::FieldRef(std::string(cmd, lkey_size)));
            lkey_sizes.push_back(lkey_size);
            cmd += lkey_size;
            total_lsize += lkey_size;

            int rkey_size = extract<int>(cmd);
            right_keys.push_back(arrow::FieldRef(std::string(cmd, rkey_size)));
            rkey_sizes.push_back(rkey_size);
            cmd += rkey_size;
            total_rsize += rkey_size;
        }

        //CkPrintf("%s, %s\n", left_keys[0].name()->c_str(), left_keys[1].name()->c_str());
        //CkPrintf("%s, %s\n", right_keys[0].name()->c_str(), right_keys[1].name()->c_str());

        arrow::acero::JoinType type = static_cast<arrow::acero::JoinType>(extract<int>(cmd));

        auto it1 = tables.find(table1);
        auto it2 = tables.find(table2);

        if (it1 != std::end(tables) && it2 != std::end(tables))
        {
            // only join locally if both tables are in the chare
            arrow::acero::HashJoinNodeOptions join_opts{
                type, left_keys, right_keys, arrow::compute::literal(true),
                "_l", "_r"
            };
            local_join(it1->second, it2->second, result_name, join_opts);
        }

        if (num_partitions > 1)
        {
            JoinTableDataMsg* msg;
            if (it2 != std::end(tables))
            {
                BufferPtr out;
                serialize(it2->second, out);
                msg = new (out->size(), total_lsize, total_rsize, nkeys * sizeof(int), 
                    nkeys * sizeof(int)) JoinTableDataMsg(
                    EPOCH, out->size(), table1, result_name, static_cast<int>(type),
                    nkeys, 1);
                JoinTableDataMsg::pack_message(msg, (char*) out->data(), left_keys, right_keys, lkey_sizes.data(), 
                    rkey_sizes.data());
            }
            else
            {
                msg = new (0, total_lsize, total_rsize, nkeys * sizeof(int), 
                    nkeys * sizeof(int)) JoinTableDataMsg(
                    EPOCH, 0, table1, result_name, static_cast<int>(type),
                    nkeys, 1);
                JoinTableDataMsg::pack_message(msg, nullptr, left_keys, right_keys, lkey_sizes.data(), rkey_sizes.data());
            }
            CkSetRefNum(msg, EPOCH);
            thisProxy[(thisIndex + 1) % num_partitions].remote_join(msg);
            // FIXME this isn't optimal because remote joins can only happen after local
            thisProxy[thisIndex].listen_remote_join();
        }
        else
            complete_operation();
    }

    void operation_print(char* cmd)
    {
        if (thisIndex == 0)
        {
            int table_name = extract<int>(cmd);
            auto it = tables.find(table_name);
            std::stringstream ss;
            if (it != std::end(tables))
            {
                arrow::PrettyPrint(*it->second, {}, &ss);
                CkPrintf("[%d] Table: %i\n%s\n", thisIndex, table_name, ss.str().c_str());
                //CkPrintf("[%d] Table: %i: %i\n", thisIndex, table_name, it->second->num_rows());
            }
                //CkPrintf("[%d]\n%s\n", thisIndex, it->second->ToString().c_str());
            else
                CkPrintf("[%d] Table not on this partition\n", thisIndex);
        }
        complete_operation();
    }

    void operation_concat(char* cmd)
    {
        int ntables = extract<int>(cmd);
        std::vector<TablePtr> concat_tables;
        for (int i = 0; i < ntables; i++)
        {
            int table = extract<int>(cmd);
            auto it = tables.find(table);

            if (it != std::end(tables))
                concat_tables.push_back(it->second);
        }

        int result = extract<int>(cmd);
        if (concat_tables.size() > 0)
            tables[result] = arrow::ConcatenateTables(concat_tables).ValueOrDie();

        complete_operation();
    }

    void operation_groupby(char* cmd)
    {
        int table = extract<int>(cmd);
        int result_name = extract<int>(cmd);
        int options_size = extract<int>(cmd);
        char* options = cmd;
        arrow::acero::AggregateNodeOptions agg_opts = extract_aggregate_options(cmd, true);
        AggregateReductionMsg* red_msg;
        int red_msg_size;

        auto it = tables.find(table);
        if (it != std::end(tables))
        {
            TablePtr result = local_aggregation(it->second, agg_opts);
            BufferPtr out;
            serialize(result, out);

            red_msg = create_reduction_msg(
                result_name, out->size(), (char*) out->data(), options_size, options
            );
            red_msg_size = sizeof(AggregateReductionMsg) + out->size() + options_size;
        }
        else
        {
            red_msg = create_reduction_msg(
                result_name, 0, nullptr, options_size, options
            );
            red_msg_size = sizeof(AggregateReductionMsg) + options_size;
        }
        CkCallback cb(CkIndex_Partition::aggregate_result(NULL), thisProxy[0]);
        contribute(red_msg_size, red_msg, AggregateReductionType, cb);
        
        // Partition 0 is complete only when the result is written to the tables map
        if (thisIndex != 0) complete_operation();
    }

    void operation_set_column(char* cmd)
    {
        int table_name = extract<int>(cmd);
        int field_size = extract<int>(cmd);
        auto it = tables.find(table_name);
        if (it != std::end(tables))
        {
            std::string field_name(cmd, field_size);
            cmd += field_size;
            arrow::Datum result = traverse_ast(cmd);
            tables[table_name] = set_column(it->second, field_name, result);
        }
        complete_operation();
    }

    void operation_filter(char* cmd)
    {
        int table_name = extract<int>(cmd);
        int result_table = extract<int>(cmd);
        auto it = tables.find(table_name);
        if (it != std::end(tables))
        {
            arrow::Datum condition = traverse_ast(cmd);
            tables[result_table] = arrow::compute::Filter(it->second, condition).ValueOrDie().table();
        }
        complete_operation();
    }

    void aggregate_result(CkReductionMsg* msg)
    {
        CkAssert(thisIndex == 0);
        AggregateReductionMsg* agg_msg = (AggregateReductionMsg*) msg->getData();
        arrow::acero::AggregateNodeOptions agg_opts = extract_aggregate_options(agg_msg->get_options());
        CkAssert(agg_msg->table_size != 0);
        tables[agg_msg->result_name] = deserialize(agg_msg->get_table(), agg_msg->table_size);
        complete_operation();
    }

    void execute_command(int epoch, int size, char* cmd)
    {
        Operation op = lookup_operation(extract<int>(cmd));

        switch (op)
        {
            case Operation::Read:
            {
                operation_read(cmd);
                break;
            }

            case Operation::Fetch:
            {
                operation_fetch(cmd);
                break;
            }

            case Operation::Join:
            {
                operation_join(cmd);
                break;
            }

            case Operation::Print:
            {
                operation_print(cmd);
                break;
            }

            case Operation::Concat:
            {
                operation_concat(cmd);
                break;
            }

            case Operation::GroupBy:
            {
                operation_groupby(cmd);
                break;
            }

            case Operation::SetColumn:
            {
                operation_set_column(cmd);
                break;
            }

            case Operation::Filter:
            {
                operation_filter(cmd);
                break;
            }

            case Operation::Rescale:
            {
                //CkPrintf("Rescale\n");
                ++EPOCH;
                AtSync();
                break;
            }

            case Operation::Skip
            {
                complete_operation();
                break;
            }
        
            default:
                break;
        }

        CkPrintf("Chare %i> Memory usage = %f MB\n", thisIndex, ((double) calculate_memory_usage()) / (1024 * 1024));
    }

    void process_remote_join(JoinTableDataMsg* msg)
    {
        if (msg->size != 0)
        {
            TablePtr t2 = deserialize(msg->data, msg->size);
            //if (thisIndex == 0)
            //CkPrintf("Received msg on %i, %i, %i\n", thisIndex, join_count, t2->num_rows());   
            auto it1 = tables.find(msg->table1_name);
            if (it1 != std::end(tables))
            {
                arrow::acero::JoinType type = static_cast<arrow::acero::JoinType>(msg->join_type);
                arrow::acero::HashJoinNodeOptions join_opts{
                    type, msg->get_left_keys(), msg->get_right_keys(), arrow::compute::literal(true),
                    "_l", "_r"
                };
                local_join(it1->second, t2, msg->result_name, join_opts);
            }
        }
        
        if (++join_count == num_partitions - 1)
        {
            join_count = 0;
            local_join_done = false;
            complete_operation();
        }
        else
        {
            thisProxy[thisIndex].listen_remote_join();
        }
        
        if (msg->fwd_count < num_partitions - 1)
        {
            // send t2 forward
            // TODO create fwd_msg - is this copy required?
            JoinTableDataMsg* fwd_msg = msg->copy();
            CkSetRefNum(fwd_msg, EPOCH);
            thisProxy[(thisIndex + 1) % num_partitions].remote_join(fwd_msg);
        }
    }

    arrow::Datum extract_operand(char* &msg)
    {
        OperandType operand_type = static_cast<OperandType>(extract<int>(msg));

        switch (operand_type)
        {
            case OperandType::Field:
            {
                int table_name = extract<int>(msg);
                int field_size = extract<int>(msg);
                std::string field_name(msg, field_size);
                msg += field_size;
                auto it = tables.find(table_name);
                if (it == std::end(tables))
                    CkAbort("Table not found\n");
                return arrow::Datum(it->second->GetColumnByName(field_name));
            }

            case OperandType::Integer:
            {
                int value = extract<int>(msg);
                ScalarPtr scalar = std::make_shared<arrow::Int64Scalar>(value);
                return arrow::Datum(scalar);
            }

            case OperandType::Double:
            {
                double value = extract<double>(msg);
                ScalarPtr scalar = std::make_shared<arrow::DoubleScalar>(value);
                return arrow::Datum(scalar);
            }
            
            default:
                return arrow::Datum();
        }
    }

    arrow::Datum traverse_ast(char* &msg)
    {
        ArrayOperation opcode = static_cast<ArrayOperation>(extract<int>(msg));

        switch (opcode)
        {
            case ArrayOperation::Noop:
            {
                return extract_operand(msg);
            }

            case ArrayOperation::Add:
            case ArrayOperation::Sub:
            case ArrayOperation::Multiply:
            case ArrayOperation::Divide:
            case ArrayOperation::LessThan:
            case ArrayOperation::LessEqual:
            case ArrayOperation::GreaterThan:
            case ArrayOperation::GreaterEqual:
            case ArrayOperation::Equal:
            case ArrayOperation::NotEqual:
            {
                std::vector<arrow::Datum> operands;
                operands.push_back(traverse_ast(msg));
                operands.push_back(traverse_ast(msg));
                return execute_operation(opcode, operands);
            }
            
            default:
                return arrow::Datum();
        }
    }

    void read_parquet(int table_name, std::string file_path)
    {
        std::shared_ptr<arrow::io::ReadableFile> input_file;
        input_file = arrow::io::ReadableFile::Open(file_path).ValueOrDie();

        // Create a ParquetFileReader instance
        std::unique_ptr<parquet::arrow::FileReader> reader;
        parquet::arrow::OpenFile(input_file, arrow::default_memory_pool(), &reader);

        // Get the file metadata
        std::shared_ptr<parquet::FileMetaData> file_metadata = reader->parquet_reader()->metadata();

        int num_rows = file_metadata->num_rows();
        int nrows_per_partition = num_rows / num_partitions;
        int start_row = nrows_per_partition * thisIndex;
        int nextra_rows = num_rows - num_partitions * nrows_per_partition;

        if (thisIndex < nextra_rows)
        {
            start_row += thisIndex;
            nrows_per_partition++;
        }
        else
        {
            start_row += nextra_rows;
        }


        int num_rows_before = start_row;
        int num_row_groups = file_metadata->num_row_groups();

        // Variables to keep track of rows read
        int64_t rows_read = 0;
        int64_t rows_to_read = nrows_per_partition;

        std::vector<TablePtr> row_tables;
        for (int i = 0; i < num_row_groups && rows_to_read > 0; ++i) 
        {
            int64_t row_group_num_rows = file_metadata->RowGroup(i)->num_rows();

            if (start_row >= row_group_num_rows) 
            {
                // Skip this row group
                start_row -= row_group_num_rows;
                continue;
            }

            // Calculate how many rows to read from this row group
            int64_t rows_in_group = std::min(rows_to_read, row_group_num_rows - start_row);

            // Read the rows
            TablePtr table;
            reader->ReadRowGroup(i, &table);
            TablePtr sliced_table = table->Slice(start_row, rows_in_group);
            row_tables.push_back(sliced_table);

            // Update counters
            rows_read += table->num_rows();
            rows_to_read -= table->num_rows();
            start_row = 0;  // Reset start_row for subsequent row groups
        }

        TablePtr combined = arrow::ConcatenateTables(row_tables).ValueOrDie();

        auto local_index_array = arrow::compute::CallFunction(
            "enumerate", 
            {combined->column(0)}
        ).ValueOrDie();

        combined = set_column(combined, "local_index", local_index_array);
        tables[table_name] = set_column(combined, "home_partition", arrow::Datum(std::make_shared<arrow::Int32Scalar>(thisIndex)));

        CkPrintf("[%d] Read number of rows = %i\n", thisIndex, combined->num_rows());
    }
};

#include "partition.def.h"
