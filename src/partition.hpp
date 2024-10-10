#include <vector>
#include <cstring>
#include <arrow/io/file.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/api.h>
#include <arrow/table.h>
#include <parquet/arrow/reader.h>
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
        //std::shared_ptr<arrow::Table> table = deserialize(data, size);
        auto it = gather_count.find(msg->epoch);
        if (it == gather_count.end())
            gather_count[msg->epoch] = 1;
        else
            gather_count[msg->epoch]++;
        gather_buffer[msg->epoch].push_back(msg);

        if (gather_count[msg->epoch] == msg->num_partitions)
        {
            std::vector<std::shared_ptr<arrow::Table>> gathered_tables;
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
            std::shared_ptr<arrow::Buffer> out;
            serialize(combined_table, out);
            fetch_callback(msg->epoch, out);
            // FIXME delete saved msgs and combined_msg here
        }
    }

    void fetch_callback(int epoch, std::shared_ptr<arrow::Buffer> &out)
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
    std::unordered_map<int, std::shared_ptr<arrow::Table>> tables;

public:
    Partition_SDAG_CODE

    Partition(int num_partitions_, CProxy_Aggregator agg_proxy_) 
        : num_partitions(num_partitions_)
        , agg_proxy(agg_proxy_)
        , EPOCH(0)
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
                    std::shared_ptr<arrow::Buffer> out;
                    serialize(table, out);
                    msg = new (out->size()) GatherTableDataMsg(epoch, out->size(), num_partitions);
                    std::memcpy(msg->data, out->data(), out->size());
                }
                else
                {
                    CkPrintf("Table not found on chare %i\n", thisIndex);
                    msg = new (0) GatherTableDataMsg(epoch, 0, num_partitions);
                    msg->data = nullptr;
                }
                agg_proxy[0].gather_table(msg);
                break;
            }
        
            default:
                break;
        }

        EPOCH++;
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

        std::vector<std::shared_ptr<arrow::Table>> row_tables;
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
            std::shared_ptr<arrow::Table> table;
            reader->ReadRowGroup(i, &table);
            std::shared_ptr<arrow::Table> sliced_table = table->Slice(start_row, rows_in_group);
            row_tables.push_back(sliced_table);

            // Update counters
            rows_read += table->num_rows();
            rows_to_read -= table->num_rows();
            start_row = 0;  // Reset start_row for subsequent row groups
        }

        std::shared_ptr<arrow::Table> combined = arrow::ConcatenateTables(row_tables).ValueOrDie();
        tables[table_name] = combined;

        CkPrintf("[%d] Read number of rows = %i\n", thisIndex, combined->num_rows());
    }
};

#include "partition.def.h"
