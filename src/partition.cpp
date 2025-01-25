#include <xxhash.h>
#include <iostream>
#include <fstream>
#include "utils.hpp"
#include "operations.hpp"
#include "messaging.hpp"
#include "reduction.hpp"
#include "serialize.hpp"
#include "partition.hpp"
#include "messaging.def.h"
#include "partition.def.h"

#define MEM_LOGGING false
#define MEM_LOG_DURATION 100

#define TEMP_TABLE_OFFSET (1 << 30)

Partition::Partition(int num_partitions_, int lb_period_, CProxy_Aggregator agg_proxy_) 
    : num_partitions(num_partitions_)
    , agg_proxy(agg_proxy_)
    , EPOCH(0)
    , lb_period(lb_period_)
{
    usesAtSync = true;

    agg_proxy.ckLocalBranch()->num_partitions = num_partitions_;
    agg_proxy.ckLocalBranch()->partition_proxy = thisProxy;
    agg_proxy.ckLocalBranch()->register_local_chare(thisIndex);

    //reduction
    int done = 1;
    CkCallback cb(CkReductionTarget(Aggregator, init_done), agg_proxy[0]);
    contribute(sizeof(int), &done, CkReduction::sum_int, cb);
}

Partition::Partition(CkMigrateMessage* m)
    : CBase_Partition(m)
{
    usesAtSync = true;
    //CkPrintf("Chare %i> Resume polling waiting for epoch = %i\n", thisIndex, EPOCH);
    //thisProxy[thisIndex].poll();
}

Partition::~Partition()
{
    // delete tables?
    tables.clear();
    tables_serialized.clear();
}

void Partition::pup(PUP::er &p)
{
    p | agg_proxy;
    p | num_partitions;
    p | EPOCH;
    p | lb_period;
    pup_tables(p);
    if (p.isUnpacking())
    {
        CkPrintf("Chare %i> Resume polling waiting for epoch = %i\n", thisIndex, EPOCH);
        thisProxy[thisIndex].poll();
    }
}

void Partition::pup_tables(PUP::er &p)
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

int64_t Partition::calculate_memory_usage()
{
    int total_size = 0;
    for (auto &it: tables)
    {
        total_size += calculate_memory_usage(it.second);
    }
    return total_size;
}

int64_t Partition::calculate_memory_usage(TablePtr table)
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

void Partition::serialize_and_cache(int table_name)
{
    BufferPtr out;
    serialize(tables[table_name], out);
    tables_serialized[table_name] = out;
}

void Partition::complete_operation()
{
    ++EPOCH;
    thisProxy[thisIndex].poll();
}

inline void Partition::inc_epoch()
{
    ++EPOCH;
}

void Partition::ResumeFromSync()
{
    //CkPrintf("Resume called\n");
    thisProxy[thisIndex].poll();
}

TablePtr Partition::get_table(int table_name)
{
    auto it = tables.find(table_name);
    if (it == std::end(tables))
        return nullptr;
    else
        return it->second;
}

TablePtr Partition::get_table(int table_name, std::vector<std::string> fields)
{
    TablePtr table = get_table(table_name);
    std::vector<int> indices;
    for (int i = 0; i < fields.size(); i++)
    {
        int col_index = table->schema()->GetFieldIndex(fields[i]);
        indices.push_back(col_index);
    }
    return table->SelectColumns(indices).ValueOrDie();
}

inline void Partition::remove_table(int table_name)
{
    tables.erase(table_name);
}

inline void Partition::add_table(int table_name, TablePtr table)
{
    arrow::Int32Builder builder;
    builder.Reserve(table->num_rows());

    for (int32_t i = 0; i < table->num_rows(); ++i)
        builder.Append(i);

    ArrayPtr index_array;
    builder.Finish(&index_array);

    table = set_column(table, "local_index", arrow::Datum(
        arrow::ChunkedArray::Make({index_array}).ValueOrDie()));
    ArrayPtr home_partition_array = arrow::MakeArrayFromScalar(
        *std::make_shared<arrow::Int32Scalar>(thisIndex),
        table->num_rows()).ValueOrDie();
    tables[table_name] = set_column(table, "home_partition", arrow::Datum(
        arrow::ChunkedArray::Make({home_partition_array}).ValueOrDie()));
}

void Partition::operation_read(char* cmd)
{
    int table_name = extract<int>(cmd);
    int path_size = extract<int>(cmd);
    std::string file_path(cmd, path_size);
    CkPrintf("[%d] Reading file: %s\n", thisIndex, file_path.c_str());
    read_parquet(table_name, file_path);
    complete_operation();
}

void Partition::operation_fetch(char* cmd)
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

void Partition::operation_print(char* cmd)
{
    if (true || thisIndex == 0)
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

void Partition::operation_concat(char* cmd)
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

void Partition::operation_groupby(char* cmd)
{
    /*int table = extract<int>(cmd);
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

    delete red_msg;
    
    // Partition 0 is complete only when the result is written to the tables map
    if (thisIndex != 0) complete_operation();*/
}

void Partition::operation_set_column(char* cmd)
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

void Partition::operation_filter(char* cmd)
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

void Partition::operation_fetch_size(char* cmd)
{
    int table_name = extract<int>(cmd);
    auto it = tables.find(table_name);
    int local_size = 0;
    if (it != std::end(tables))
        local_size = it->second->num_rows();
    agg_proxy[0].deposit_size(thisIndex, local_size);
    complete_operation();
}

void Partition::aggregate_result(CkReductionMsg* msg)
{
    /*CkAssert(thisIndex == 0);
    AggregateReductionMsg* agg_msg = (AggregateReductionMsg*) msg->getData();
    arrow::acero::AggregateNodeOptions agg_opts = extract_aggregate_options(agg_msg->get_options());
    CkAssert(agg_msg->table_size != 0);
    tables[agg_msg->result_name] = deserialize(agg_msg->get_table(), agg_msg->table_size);
    complete_operation();*/
}

void Partition::handle_deletions(char* &cmd)
{
    int num_deletions = extract<int>(cmd);
    for (int i = 0; i < num_deletions; i++)
    {
        int table_name = extract<int>(cmd);
        remove_table(table_name);
    }
}

void Partition::execute_command(int epoch, int size, char* cmd)
{
    handle_deletions(cmd);
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

        case Operation::Skip:
        {
            complete_operation();
            break;
        }

        case Operation::FetchSize:
        {
            operation_fetch_size(cmd);
            break;
        }
    
        default:
            break;
    }

    //CkPrintf("Chare %i> Memory usage = %f MB\n", thisIndex, ((double) calculate_memory_usage()) / (1024 * 1024));
}

arrow::Datum Partition::extract_operand(char* &msg)
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

arrow::Datum Partition::traverse_ast(char* &msg)
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

void Partition::read_parquet(int table_name, std::string file_path)
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

    arrow::Int32Builder builder;
    builder.Reserve(combined->num_rows());

    for (int32_t i = 0; i < combined->num_rows(); ++i)
        builder.Append(i);

    ArrayPtr index_array;
    builder.Finish(&index_array);

    combined = set_column(combined, "local_index", arrow::Datum(
        arrow::ChunkedArray::Make({index_array}).ValueOrDie()));
    ArrayPtr home_partition_array = arrow::MakeArrayFromScalar(
        *std::make_shared<arrow::Int32Scalar>(thisIndex), 
        combined->num_rows()).ValueOrDie();
    tables[table_name] = set_column(combined, "home_partition", arrow::Datum(
        arrow::ChunkedArray::Make({home_partition_array}).ValueOrDie()));

    CkPrintf("[%d] Read number of rows = %i\n", thisIndex, combined->num_rows());
}

Aggregator::Aggregator(CProxy_Main main_proxy_)
        : main_proxy(main_proxy_)
        , num_local_chares(0)
        , join_opts(nullptr)
        , groupby_opts(nullptr)
        , redist_odf(8)
        , expected_rows(0)
        , EPOCH(0)
{
    if (MEM_LOGGING)
    {
        init_memory_logging();
        CcdCallFnAfter((CcdVoidFn) log_memory_usage, nullptr, MEM_LOG_DURATION);
    }
}

Aggregator::Aggregator(CkMigrateMessage* m) 
        : CBase_Aggregator(m)
        , expected_rows(0)
        , join_opts(nullptr)
        , groupby_opts(nullptr)
{
    if (MEM_LOGGING)
    {
        init_memory_logging();
        CcdCallFnAfter((CcdVoidFn) log_memory_usage, nullptr, MEM_LOG_DURATION);
    }
}

void Aggregator::pup(PUP::er &p) 
{
    p | main_proxy;
    p | redist_odf;
    p | next_temp_name;
    p | EPOCH;
}

void Aggregator::init_memory_logging()
{
    char filename[20];
    sprintf(filename, "memory_log_%i.txt", CkMyPe());
    std::ofstream outfile;
    outfile.open(filename, std::ofstream::out | std::ofstream::trunc);    
}

void Aggregator::log_memory_usage(void* msg, double curr_time)
{
    char filename[20];
    sprintf(filename, "memory_log_%i.txt", CkMyPe());
    std::ofstream outfile;
    outfile.open(filename, std::ios_base::app); // append instead of overwrite
    outfile << curr_time << "," << CmiMemoryUsage() << "\n";
    CcdCallFnAfter((CcdVoidFn) log_memory_usage, nullptr, MEM_LOG_DURATION);
}

void Aggregator::init_done()
{
    main_proxy.init_done();
}

void Aggregator::register_local_chare(int index)
{
    num_local_chares++;
    local_chares.push_back(index);
    local_chares_set.insert(index);
}

void Aggregator::gather_table(GatherTableDataMsg* msg)
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

void Aggregator::clear_gather_buffer(int epoch)
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

void Aggregator::deposit_size(int partition, int local_size)
{
    //if (table_sizes)
}

void Aggregator::fetch_callback(int epoch, BufferPtr &out)
{
    CkAssert(CkMyPe() == 0);
    CcsSendDelayedReply(fetch_reply[epoch], out->size(), (char*) out->data());
}

TablePtr Aggregator::get_local_table(int table_name)
{
    TablePtr local_table = nullptr;
    for (int i = 0; i < num_local_chares; i++)
    {
        int index = local_chares[i];
        TablePtr part_table = partition_proxy[index].ckLocal()->get_table(table_name);
        if (part_table != nullptr)
        {
            if (local_table == nullptr)
                local_table = part_table;
            else
                local_table = arrow::ConcatenateTables({local_table, part_table}).ValueOrDie();
        }
    }

    return local_table;
}

void Aggregator::operation_groupby(char* cmd)
{
    int table_name = extract<int>(cmd);
    int result_name = extract<int>(cmd);
    int options_size = extract<int>(cmd);
    char* options = cmd;
    arrow::acero::AggregateNodeOptions* agg_opts = extract_aggregate_options(cmd, true);

    groupby_opts = new GroupByOptions(table_name, result_name, agg_opts);

    TablePtr local_table = get_local_table(table_name);
    tables[table_name] = local_table;

    TablePtr result = local_table;
    if (is_two_level_agg(*agg_opts))
    {
        if (local_table != nullptr)
            result = local_aggregation(local_table, *agg_opts);
    }

    tables[TEMP_TABLE_OFFSET + next_temp_name] = result;
    auto redist_keys = std::vector<std::vector<arrow::FieldRef>>{agg_opts->keys};
    auto table_names = std::vector<int>{TEMP_TABLE_OFFSET + next_temp_name};
    redistribute(table_names, redist_keys, RedistOperation::GroupBy);
}

void Aggregator::operation_join(char* cmd)
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

    // only join locally if both tables are in the chare
    arrow::acero::HashJoinNodeOptions* opts = new arrow::acero::HashJoinNodeOptions(
        type, left_keys, right_keys, arrow::compute::literal(true),
        "_l", "_r"
    );
    join_opts = new JoinOptions(table1, table2, result_name, opts);

    start_join();
}

void Aggregator::handle_deletions(char* &cmd)
{
    char* del_cmd;
    for (int i = 0; i < num_local_chares; i++)
    {
        int index = local_chares[i];
        del_cmd = cmd;
        partition_proxy[index].ckLocal()->handle_deletions(del_cmd);
    }

    cmd = del_cmd;
}

void Aggregator::execute_command(int epoch, int size, char* cmd)
{
    handle_deletions(cmd);
    Operation op = lookup_operation(extract<int>(cmd));

    switch (op)
    {
        case Operation::Join:
        {
            operation_join(cmd);
            break;
        }

        case Operation::GroupBy:
        {
            operation_groupby(cmd);
            break;
        }

        default:
            break;
    }
}

void Aggregator::update_histogram(TablePtr table, std::vector<int> &hist)
{
    ChunkedArrayPtr key_array = table->GetColumnByName("_mapped_key");

    for (int i = 0; i < key_array->length(); i++)
    {
        uint32_t key = std::dynamic_pointer_cast<arrow::UInt32Scalar>(key_array->GetScalar(i).ValueOrDie())->value;
        hist[key]++;
    }
}

TablePtr Aggregator::map_keys(TablePtr &table, std::vector<arrow::FieldRef> &fields)
{
    std::vector<ChunkedArrayPtr> field_arrays;
    for (int i = 0; i < fields.size(); i++)
        field_arrays.push_back(table->GetColumnByName(*(fields[i].name())));

    ArrayPtr hash_array;

    arrow::UInt32Builder builder;
    builder.Reserve(table->num_rows());

    for (int i = 0; i < table->num_rows(); i++)
    {
        XXH32_state_t* hash_state = XXH32_createState();
        XXH32_reset(hash_state, 0);

        for (int j = 0; j < field_arrays.size(); j++)
        {
            ChunkedArrayPtr arr = field_arrays[j];
            if (arr->type()->id() == arrow::Type::STRING)
            {
                std::string key = std::dynamic_pointer_cast<arrow::StringScalar>(arr->GetScalar(i).ValueOrDie())->value->ToString();
                XXH32_update(hash_state, key.c_str(), key.size());
            }
            else if (arr->type()->id() == arrow::Type::INT32)
            {
                int32_t key = std::dynamic_pointer_cast<arrow::Int32Scalar>(arr->GetScalar(i).ValueOrDie())->value;
                XXH32_update(hash_state, &key, sizeof(int32_t));
            }
            else
            {
                CkAbort("Illegal type for join keys");
            }
        }

        XXH32_hash_t final_hash = XXH32_digest(hash_state);

        builder.Append(static_cast<uint32_t>(final_hash) % (redist_odf * CkNumPes()));
    }

    builder.Finish(&hash_array);
    return set_column(table, "_mapped_key", arrow::Datum(arrow::ChunkedArray::Make({hash_array}).ValueOrDie()));
}

void Aggregator::start_join()
{
    tables[join_opts->table1] = get_local_table(join_opts->table1);
    tables[join_opts->table2] = get_local_table(join_opts->table2);

    auto redist_keys = std::vector<std::vector<arrow::FieldRef>>{
        join_opts->opts->left_keys, join_opts->opts->right_keys};
    redistribute({join_opts->table1, join_opts->table2}, 
        redist_keys, RedistOperation::Join);
}

void Aggregator::redistribute(std::vector<int> table_names, std::vector<std::vector<arrow::FieldRef>> &keys,
    RedistOperation oper)
{
    redist_table_names = table_names;
    redist_operation = oper;
    for (int i = 0; i < table_names.size(); i++)
    {
        if (tables[table_names[i]] != nullptr)
            tables[table_names[i]] = map_keys(tables[table_names[i]], keys[i]);
    }

    std::vector<int> local_hist(redist_odf * CkNumPes(), 0);
    for (int i = 0; i < table_names.size(); i++)
    {
        if (tables[table_names[i]] != nullptr)
            update_histogram(tables[table_names[i]], local_hist);
    }

    CkCallback cb(CkReductionTarget(Aggregator, assign_keys), thisProxy[0]);
    contribute(redist_odf * CkNumPes() * sizeof(int), local_hist.data(), CkReduction::sum_int, cb);
}

void Aggregator::assign_keys(int num_elements, int* global_hist)
{
    // this is where the result of the global
    // histogram goes
    std::vector<int> keys(num_elements);
    std::vector<int> sorted_hist(num_elements);
    std::iota(keys.begin(), keys.end(), 0);

    std::sort(keys.begin(), keys.end(),
              [&global_hist](int i, int j) { return global_hist[i] < global_hist[j];});

    for (int i = 0; i < keys.size(); i++)
        sorted_hist[i] = global_hist[keys[i]];

    // assign buckets to PEs and bcast mapping
    std::vector<int> pe_map(num_elements);
    std::vector<int> expected_loads(num_elements, 0);
    std::priority_queue<PELoad, std::vector<PELoad>, std::greater<PELoad>> pe_loads;

    for (int i = 0; i < CkNumPes(); i++)
        pe_loads.push(PELoad(i, 0));

    for (int i = 0; i < num_elements; i++)
    {
        PELoad pe_load = pe_loads.top();
        pe_loads.pop();
        pe_map[keys[i]] = pe_load.pe;
        pe_load.load += sorted_hist[i];
        expected_loads[pe_load.pe] += sorted_hist[i];
        pe_loads.push(pe_load);
    }

    CkPrintf("Loads after reshuffling:\n");
    for (int i = 0; i < CkNumPes(); i++)
        CkPrintf("%i, ", expected_loads[i]);
    CkPrintf("\n");

    thisProxy.shuffle_data(pe_map, expected_loads);
}

void Aggregator::shuffle_data(std::vector<int> pe_map, std::vector<int> expected_loads)
{
    // result of bcast goes here
    // shuffle data based on data mapping
    expected_rows = expected_loads[CkMyPe()];

    std::vector<BufferPtr> serialized_buffers[CkNumPes()];
    int total_size[CkNumPes()];
    for (int i = 0; i < CkNumPes(); i++)
        total_size[i] = 0;

    for (int i = 0; i < redist_table_names.size(); i++)
    {
        int table_name = redist_table_names[i];
        TablePtr table = tables[table_name];
        std::vector<int> indices[CkNumPes()];

        //CkPrintf("%i: %s\n", table_name, table->schema()->ToString().c_str());
        ChunkedArrayPtr key_array = table->GetColumnByName("_mapped_key");

        for (int j = 0; j < key_array->length(); j++)
        {
            int pe = pe_map[std::dynamic_pointer_cast<arrow::UInt32Scalar>(key_array->GetScalar(j).ValueOrDie())->value];
            indices[pe].push_back(j);
        }

        for (int j = 0; j < CkNumPes(); j++)
        {
            if (indices[j].size() > 0)
            {
                ChunkedArrayPtr indices_array = array_from_vector(indices[j]);
                TablePtr selected = arrow::compute::Take(table, indices_array).ValueOrDie().table();
                if (j == CkMyPe())
                {
                    auto it = redist_tables.find(table_name);
                    if (it == std::end(redist_tables))
                        redist_tables[table_name] = selected;
                    else
                        it->second = arrow::ConcatenateTables({it->second, selected}).ValueOrDie();
                }
                else
                {
                    BufferPtr out;
                    serialize(selected, out);
                    serialized_buffers[j].push_back(out);
                    total_size[j] += out->size();
                }
            }
            else
            {
                serialized_buffers[j].push_back(nullptr);
            }
        }
    }

    for (int i = 0; i < CkNumPes(); i++)
    {
        if (i != CkMyPe() && total_size[i] > 0)
        {
            // build redist message
            RedistTableMsg* msg = new (total_size[i], serialized_buffers[i].size()) RedistTableMsg(
                serialized_buffers[i], total_size[i]);
            thisProxy[i].receive_shuffle_data(msg);
        }
    }

    for (int i = 0; i < redist_table_names[i]; i++)
    {
        int table_name = redist_table_names[i];
        tables.erase(table_name);

        for (int i = 0; i < num_local_chares; i++)
        {
            int index = local_chares[i];
            partition_proxy[index].ckLocal()->remove_table(table_name);
        }
    }

    int total_redist_rows = 0;
    for (int i = 0; i < redist_table_names.size(); i++)
    {
        int table_name = redist_table_names[i];
        auto it = redist_tables.find(table_name);
        if (it != std::end(redist_tables))
            total_redist_rows += it->second->num_rows();
    }

    if (expected_rows != -1 && total_redist_rows == expected_rows)
    {
        for (int i = 0; i < redist_table_names.size(); i++)
        {
            int table_name = redist_table_names[i];
            tables[table_name] = redist_tables[table_name];
            redist_tables.erase(table_name);
        }
        redist_callback();
    }
}

void Aggregator::redist_callback()
{
    switch (redist_operation)
    {
        case RedistOperation::Join:
        {
            join_callback();
            break;
        }

        case RedistOperation::GroupBy:
        {
            groupby_callback();
            break;
        }

        default:
            CkAbort("Redist callback not found!");
    }
}

void Aggregator::groupby_callback()
{
    // update groupby options
    if (is_two_level_agg(*groupby_opts->opts))
    {
        for (int i = 0; i < groupby_opts->opts->aggregates.size(); i++)
        {
            groupby_opts->opts->aggregates[i].function = aggregation_callback_fn(
                groupby_opts->opts->aggregates[i].function);
            groupby_opts->opts->aggregates[i].target = std::vector<arrow::FieldRef>{
                arrow::FieldRef(groupby_opts->opts->aggregates[i].name)};
        }
    }

    TablePtr result = local_aggregation(tables[TEMP_TABLE_OFFSET + next_temp_name], *groupby_opts->opts);
    result = clean_metadata(result);
    partition_table(result, groupby_opts->result_name);
    complete_groupby();
}

void Aggregator::join_callback()
{
    TablePtr result = local_join(tables[join_opts->table1], tables[join_opts->table2], *join_opts->opts);
    CkPrintf("Mem usage before clean metadata = %d\n", CmiMemoryUsage());
    result = clean_metadata(result);
    CkPrintf("Mem usage after clean metadata = %d\n", CmiMemoryUsage());
    partition_table(tables[join_opts->table1], join_opts->table1);
    partition_table(tables[join_opts->table2], join_opts->table2);
    partition_table(result, join_opts->result_name);
    complete_join();
}

TablePtr Aggregator::clean_metadata(TablePtr &table)
{
    std::vector<int> indices;
    std::vector<std::string> field_names = table->schema()->field_names();
    for (int i = 0; i < field_names.size(); i++)
    {
        if (field_names[i].rfind("local_index_", 0) == 0 ||
            field_names[i].rfind("home_partition_", 0) == 0 ||
            field_names[i].rfind("_mapped_key_", 0) == 0)
            continue;
        int col_index = table->schema()->GetFieldIndex(field_names[i]);
        indices.push_back(col_index);
    }
    return table->SelectColumns(indices).ValueOrDie();
}

void Aggregator::receive_shuffle_data(RedistTableMsg* msg)
{
    std::vector<TablePtr> remote_tables = msg->get_tables();

    int total_redist_rows = 0;
    for (int i = 0; i < remote_tables.size(); i++)
    {
        int table_name = redist_table_names[i];
        auto it = redist_tables.find(table_name);
        if (it == std::end(redist_tables))
            redist_tables[table_name] = remote_tables[i];
        else
            it->second = arrow::ConcatenateTables({it->second, remote_tables[i]}).ValueOrDie();
        total_redist_rows += redist_tables[table_name]->num_rows();
    }

    if (expected_rows != -1 && total_redist_rows == expected_rows)
    {
        for (int i = 0; i < redist_table_names.size(); i++)
        {
            int table_name = redist_table_names[i];
            tables[table_name] = redist_tables[table_name];
            redist_tables.erase(table_name);
        }
        redist_callback();
    }
}

void Aggregator::complete_operation()
{
    redist_table_names.clear();
    tables.clear();
    redist_tables.clear();

    EPOCH++;
    thisProxy[thisIndex].poll();
}

void Aggregator::complete_groupby()
{
    CkPrintf("PE%i> Completed groupby\n", CkMyPe());
    tables.erase(groupby_opts->table_name);
    delete groupby_opts->opts;
    delete groupby_opts;
    groupby_opts = nullptr;
    //EPOCH++;
    tables.erase(TEMP_TABLE_OFFSET + next_temp_name++);

    for (int i = 0; i < num_local_chares; i++)
    {
        int index = local_chares[i];
        partition_proxy[index].ckLocal()->complete_operation();
    }

    complete_operation();
}

void Aggregator::complete_join()
{
    CkPrintf("PE%i> Completed join\n", CkMyPe());
    tables.erase(join_opts->table1);
    tables.erase(join_opts->table2);
    delete join_opts->opts;
    delete join_opts;
    join_opts = nullptr;
    //EPOCH++;

    for (int i = 0; i < num_local_chares; i++)
    {
        int index = local_chares[i];
        partition_proxy[index].ckLocal()->complete_operation();
    }

    complete_operation();
}

TablePtr Aggregator::local_join(TablePtr &t1, TablePtr &t2, arrow::acero::HashJoinNodeOptions &opts)
{
    arrow::acero::Declaration left{"table_source", arrow::acero::TableSourceNodeOptions(t1)};
    arrow::acero::Declaration right{"table_source", arrow::acero::TableSourceNodeOptions(t2)};

    arrow::acero::Declaration hashjoin{"hashjoin", {std::move(left), std::move(right)}, opts};

    // Collect the results
    return arrow::acero::DeclarationToTable(std::move(hashjoin)).ValueOrDie();
}

void Aggregator::partition_table(TablePtr table, int result_name)
{
    // partition table into local chares
    int remain_chares = num_local_chares;
    int remain_rows = table->num_rows();
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