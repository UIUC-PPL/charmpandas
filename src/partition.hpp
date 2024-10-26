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
};


class Partition : public CBase_Partition
{
private:
    CProxy_Aggregator agg_proxy;
    int num_partitions;
    int EPOCH;
    int join_count;
    bool local_join_done;
    std::unordered_map<int, TablePtr> tables;

public:
    Partition_SDAG_CODE

    Partition(int num_partitions_, CProxy_Aggregator agg_proxy_) 
        : num_partitions(num_partitions_)
        , agg_proxy(agg_proxy_)
        , EPOCH(0)
        , join_count(0)
        , local_join_done(false)
    {}

    Partition(CkMigrateMessage* m) {}

    ~Partition()
    {
        // delete tables?
    }

    void complete_operation()
    {
        EPOCH++;
        thisProxy[thisIndex].poll();
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
        
            default:
                break;
        }
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

    void local_join(TablePtr &t1, TablePtr &t2, int result_name, arrow::acero::HashJoinNodeOptions &join_opts)
    {
        // join t1 and t2 and concatenate to result
        arrow::acero::Declaration left{"table_source", arrow::acero::TableSourceNodeOptions(t1)};
        arrow::acero::Declaration right{"table_source", arrow::acero::TableSourceNodeOptions(t2)};

        arrow::acero::Declaration hashjoin{"hashjoin", {std::move(left), std::move(right)}, join_opts};

        // Collect the results
        TablePtr result_table = arrow::acero::DeclarationToTable(std::move(hashjoin)).ValueOrDie();

        auto it = tables.find(result_name);
        if (it != std::end(tables))
        {
            it->second = arrow::ConcatenateTables({it->second, result_table}).ValueOrDie();
        }
        else
        {
            tables[result_name] = result_table;
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
