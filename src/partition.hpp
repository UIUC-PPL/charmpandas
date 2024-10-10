#include <vector>
#include <cstring>
#include <arrow/io/file.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/api.h>
#include <arrow/table.h>
#include <arrow/compute/api.h>
#include <parquet/arrow/reader.h>
#include "arrow/acero/exec_plan.h"
#include "utils.hpp"
#include "serialize.hpp"
#include "partition.decl.h"


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


class JoinTableDataMsg : public BaseTableDataMsg, public CMessage_JoinTableDataMsg
{
public:
    int table1_name;
    int result_name;
    int join_type;
    std::string left_key;
    std::string right_key;

    JoinTableDataMsg(int epoch_, int size_, int table1_name_, int result_name_, int join_type_,
        std::string left_key_, std::string right_key_)
        : BaseTableDataMsg(epoch_, size_)
        , table1_name(table1_name_)
        , result_name(result_name_)
        , join_type(join_type_)
        , left_key(left_key_)
        , right_key(right_key_)
    {}
};


// This should not be a group!
class Aggregator : public CBase_Aggregator
{
private:
    std::unordered_map<int, int> gather_count;
    std::unordered_map<int, std::vector<GatherTableDataMsg*>> gather_buffer;

public:
    Aggregator() {}

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
                if (buff_msg->data == nullptr)
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
        for (int i = 0; i < gather_buffer.size(); i++)
        {
            msg = gather_buffer[epoch];
            delete msg;
        }
        gather_buffer[epoch].clear();
    }

    void fetch_callback(int epoch, BufferPtr &out)
    {
        CkAssert(CkMyPe() == 0);
        CcsSendDelayedReply(fetch_reply[epoch], out->size(), (char*) out->data());
    }
};


class Partition : public CBase_Partition
{
private:
    CProxy_Aggregator agg_proxy;
    int num_partitions;
    int EPOCH;
    int join_count;
    std::unordered_map<int, TablePtr> tables;

public:
    Partition_SDAG_CODE

    Partition(int num_partitions_, CProxy_Aggregator agg_proxy_) 
        : num_partitions(num_partitions_)
        , agg_proxy(agg_proxy_)
        , EPOCH(0)
        , join_count(0)
    {}

    Partition(CkMigrateMessage* m) {}

    ~Partition()
    {
        // delete tables?
    }

    void execute_command(int epoch, int size, char* cmd)
    {
        Operation op = lookup_operation(extract<int>(cmd));

        switch (op)
        {
            case Operation::Read:
            {
                int table_name = extract<int>(cmd);
                int path_size = extract<int>(cmd);
                std::string file_path(cmd, path_size);
                CkPrintf("[%d] Reading file: %s\n", thisIndex, file_path.c_str());
                read_parquet(table_name, file_path);
                EPOCH++;
                break;
            }

            case Operation::Fetch:
            {
                int table_name = extract<int>(cmd);
                auto it = tables.find(table_name);
                GatherTableDataMsg* msg;
                if (it != std::end(tables))
                {
                    auto table = tables[table_name];
                    BufferPtr out;
                    serialize(table, out);
                    msg = new (out->size()) GatherTableDataMsg(epoch, out->size(), num_partitions);
                    std::memcpy(msg->data, out->data(), out->size());
                }
                else
                {
                    //CkPrintf("Table not found on chare %i\n", thisIndex);
                    msg = new (0) GatherTableDataMsg(epoch, 0, num_partitions);
                    msg->data = nullptr;
                }
                agg_proxy[0].gather_table(msg);
                EPOCH++;
                break;
            }

            case Operation::Join:
            {
                int table1 = extract<int>(cmd);
                int table2 = extract<int>(cmd);
                int result_name = extract<int>(cmd);

                int key_size = extract<int>(cmd);
                std::string left_key(cmd, key_size);
                cmd += key_size

                key_size = extract<int>(cmd);
                std::string right_key(cmd, key_size);
                cmd += key_size;

                arrow::acero::JoinType type = static_cast<arrow::acero::JoinType>(extract<int>(cmd));

                auto it1 = tables.find(table1);
                auto it2 = tables.find(table2);

                if (it1 != std::end(tables) && it2 != std::end(tables))
                {
                    // only join locally if both tables are in the chare
                    arrow::acero::HashJoinNodeOptions join_opts{
                        type, {left_key}, {right_key}, arrow::compute::literal(true),
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
                        msg = new (out->size()) JoinTableDataMsg(
                            epoch, out->size(), table1, result_name, type,
                            left_key, right_key);
                        std::memcpy(msg->data, out->data(), out->size());
                    }
                    else
                    {
                        msg = new (0) JoinTableDataMsg(epoch, 0, table1, result_name, 
                            type, left_key, right_key);
                        msg->data = nullptr;
                    }
                    thisProxy[(thisIndex + 1) % num_partitions].remote_join(msg);
                }
            }

            case Operation::Print:
            {
                int table_name = extract<int>(cmd);
                auto it = tables.find(table_name);
                if (it != std::end(tables))
                    CkPrintf("[%d]\n%s\n", thisIndex, it->second->ToString().c_str());
                else
                    CkPrintf("[%d]Table not on this partition\n");
                EPOCH++;
            }
        
            default:
                break;
        }
    }

    void remote_join(JoinTableDataMsg* msg)
    {
        if (msg->data != nullptr)
        {
            TablePtr t2 = deserialize(msg->data, msg->size);
            
            auto it1 = tables.find(msg->table1_name);
            if (it1 != std::end(tables))
            {
                arrow::acero::JoinType type = static_cast<arrow::acero::JoinType>(msg->join_type);
                arrow::acero::HashJoinNodeOptions join_opts{
                    type, {msg->left_key}, {msg->right_key}, arrow::compute::literal(true),
                    "_l", "_r"
                };
                local_join(it1->second, t2, msg->result_name, join_opts);
            }
        }

        if (++join_count == num_partitions)
        {
            EPOCH++;
            join_count = 0;
        }
        else
        {
            // send t2 forward
            // TODO create fwd_msg
            thisProxy[(thisIndex + 1) % num_partitions].remote_join(fwd_msg);
        }
    }

    void local_join(TablePtr &t1, TablePtr &t2, int result_name, arrow::acero::HashJoinNodeOptions &join_opts)
    {
        // join t1 and t2 and concatenate to result
        arrow::acero::Declaration left{"table_source", arrow::acero::TableSourceNodeOptions(t1)};
        arrow::acero::Declaration right{"table_source", arrow::acero::TableSourceNodeOptions(t2)};

        arrow::acero::Declaration hashjoin{"hashjoin", {std::move(left), std::move(right), join_opts}};

        // Collect the results
        TablePtr result_table;
        result_table = arrow::acero::DeclarationToTable(std::move(hashjoin));

        auto it = tables.find(result_name);
        if (it != std::end(tables))
        {
            it->second = arrow::ConcatenateTables({it->second, result_table});
        }
        else
        {
            tables[result_name] = result_table;
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
        tables[table_name] = combined;

        CkPrintf("[%d] Read number of rows = %i\n", thisIndex, combined->num_rows());
    }
};

#include "partition.def.h"
