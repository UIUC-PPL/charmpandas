#ifndef USE_GPU
#include <xxhash.h>
#endif
#include <iostream>
#include <fstream>
#include <filesystem>
#include <regex>
#include <filesystem>
#include <regex>
#include "utils.hpp"
#include "operations.hpp"
#include "messaging.hpp"
#include "reduction.hpp"
#include "serialize.hpp"
#include "partition.hpp"
#include "messaging.def.h"
#define CK_TEMPLATES_ONLY
#include "partition.def.h"
#undef CK_TEMPLATES_ONLY
#include "partition.def.h"

#define MEM_LOGGING false
#define MEM_LOGGING false
#define MEM_LOG_DURATION 100

#define TEMP_TABLE_OFFSET (1 << 30)

namespace fs = std::filesystem;


#ifdef USE_GPU
// GPU clean_metadata is defined in gpu_ops.hpp as gpu_clean_metadata()
#else
TablePtr clean_metadata(TablePtr &table)
{
    std::vector<int> indices;
    std::vector<std::string> field_names = table->schema()->field_names();
    for (int i = 0; i < field_names.size(); i++)
    {
        if (field_names[i].rfind("local_index", 0) == 0 ||
            field_names[i].rfind("home_partition", 0) == 0 ||
            field_names[i].rfind("_mapped_key", 0) == 0)
            continue;
        int col_index = table->schema()->GetFieldIndex(field_names[i]);
        indices.push_back(col_index);
    }
    return table->SelectColumns(indices).ValueOrDie();
}
#endif

std::string get_parent_dir(const std::string& path_str) 
{
    std::filesystem::path path(path_str);
    return path.remove_filename().string();
}

std::vector<std::string> get_matching_files(std::string& path) 
{
    fs::path dir(get_parent_dir(path));
    std::regex pattern(path);
    std::vector<std::string> matches;
    for (const auto& entry : fs::recursive_directory_iterator(dir)) {
        if (fs::is_regular_file(entry) && 
            std::regex_match(entry.path().string(), pattern)) {
            matches.push_back(entry.path().string());
        }
    }
    return matches;
}


template<typename T>
void Partition::reduce_scalar(ScalarPtr& scalar, AggregateOperation& op)
{
    // Null check
    if (!scalar->is_valid) {
        CkAbort("Scalar is null");
    }

    switch (scalar->type->id())
    {
        case arrow::Type::INT32:
        {
            auto primitive_scalar = std::static_pointer_cast<arrow::Int32Scalar>(scalar);
            CkCallback cb(CkReductionTarget(Partition, reduction_result_int), thisProxy[0]);
            contribute(sizeof(T), (void*) &primitive_scalar->value, get_reduction_function(op, scalar->type->id()), cb);
            break;
        }

        case arrow::Type::INT64:
        {
            auto primitive_scalar = std::static_pointer_cast<arrow::Int64Scalar>(scalar);
            CkCallback cb(CkReductionTarget(Partition, reduction_result_long), thisProxy[0]);
            contribute(sizeof(T), (void*) &primitive_scalar->value, get_reduction_function(op, scalar->type->id()), cb);
            break;
        }

        case arrow::Type::TIMESTAMP:
        {
            auto primitive_scalar = std::static_pointer_cast<arrow::TimestampScalar>(scalar);
            CkCallback cb(CkReductionTarget(Partition, reduction_result_long), thisProxy[0]);
            contribute(sizeof(T), (void*) &primitive_scalar->value, get_reduction_function(op, scalar->type->id()), cb);
            break;
        }

        case arrow::Type::FLOAT:
        {
            auto primitive_scalar = std::static_pointer_cast<arrow::FloatScalar>(scalar);
            CkCallback cb(CkReductionTarget(Partition, reduction_result_float), thisProxy[0]);
            contribute(sizeof(T), (void*) &primitive_scalar->value, get_reduction_function(op, scalar->type->id()), cb);
            break;
        }

        default:
        {
            CkAbort("Reduction over unsupported datatype %s\n", scalar->type->ToString().c_str());
        }
    }
}


//template<typename T>
void Partition::reduction_result_int(int result)
{
    agg_proxy[0].reduction_result_int(result, EPOCH - 1);
}


void Partition::reduction_result_long(int64_t result)
{
    agg_proxy[0].reduction_result_long(result, EPOCH - 1);
}


void Partition::reduction_result_float(float result)
{
    agg_proxy[0].reduction_result_float(result, EPOCH - 1);
}


Partition::Partition(int num_partitions_, int lb_period_, CProxy_Aggregator agg_proxy_)
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
    /*if (p.isPacking())
    {
        // deregister this chare with the aggregator
        if (agg_proxy.ckLocalBranch() != NULL)
            agg_proxy.ckLocalBranch()->deregister_local_chare(thisIndex);
    }
    else if (p.isUnpacking())
    /*if (p.isPacking())
    {
        // deregister this chare with the aggregator
        if (agg_proxy.ckLocalBranch() != NULL)
            agg_proxy.ckLocalBranch()->deregister_local_chare(thisIndex);
    }
    else if (p.isUnpacking())
    {
        // Register local chares with the aggregator
        agg_proxy.ckLocalBranch()->register_local_chare(thisIndex);
        //CkPrintf("Chare %i> Resume polling waiting for epoch = %i\n", thisIndex, EPOCH);
        //thisProxy[thisIndex].poll();
    }*/
        // Register local chares with the aggregator
        agg_proxy.ckLocalBranch()->register_local_chare(thisIndex);
        //CkPrintf("Chare %i> Resume polling waiting for epoch = %i\n", thisIndex, EPOCH);
        //thisProxy[thisIndex].poll();
    }*/
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
            char* buf = new char[serialized_size];
            p(buf, serialized_size);
            tables[table_name] = deserialize(buf, serialized_size);
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
#ifdef USE_GPU
    // Estimate GPU memory: sum of column data sizes
    int64_t total_size = 0;
    for (int i = 0; i < table->num_columns(); ++i)
    {
        auto col = table->view().column(i);
        total_size += col.size() * cudf::size_of(col.type());
        if (col.null_mask())
            total_size += cudf::bitmask_allocation_size_bytes(col.size());
    }
    return total_size;
#else
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
#endif
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
    CkPrintf("Resume called\n");
    agg_proxy.ckLocalBranch()->register_local_chare(thisIndex);

    int done = 1;
    CkCallback cb(CkReductionTarget(Aggregator, start_polling), agg_proxy[0]);
    contribute(sizeof(int), &done, CkReduction::sum_int, cb);
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
#ifdef USE_GPU
    std::vector<int> indices;
    for (int i = 0; i < fields.size(); i++)
        indices.push_back(table->GetColumnIndex(fields[i]));
    return table->SelectColumns(indices);
#else
    std::vector<int> indices;
    for (int i = 0; i < fields.size(); i++)
    {
        int col_index = table->schema()->GetFieldIndex(fields[i]);
        indices.push_back(col_index);
    }
    return table->SelectColumns(indices).ValueOrDie();
#endif
}

inline void Partition::remove_table(int table_name)
{
    tables.erase(table_name);
}

inline void Partition::add_table(int table_name, TablePtr table)
{
#ifdef USE_GPU
    tables[table_name] = gpu_add_metadata(table, thisIndex);
#else
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
#endif
}

void Partition::operation_read(char* cmd)
{
    int table_name = extract<int>(cmd);
    int path_size = extract<int>(cmd);
    std::string file_path(cmd, path_size);
    if (thisIndex == 0)
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
#ifdef USE_GPU
        auto table = gpu_clean_metadata(tables[table_name]);
#else
        auto table = clean_metadata(tables[table_name]);
#endif
        BufferPtr out;
        serialize(table, out);
        msg = new (out->size()) GatherTableDataMsg(EPOCH, out->size(), thisIndex, num_partitions);
        std::memcpy(msg->data, out->data(), out->size());
    }
    else
    {
        msg = new (0) GatherTableDataMsg(EPOCH, 0, thisIndex, num_partitions);
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
#ifdef USE_GPU
            // Convert to Arrow for printing (debug path, ok to be slow)
            auto arrow_table = it->second->to_arrow();
            arrow::PrettyPrint(*arrow_table, {}, &ss);
#else
            arrow::PrettyPrint(*it->second, {}, &ss);
#endif
            CkPrintf("[%d] Table: %i\n%s\n", thisIndex, table_name, ss.str().c_str());
        }
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
    {
#ifdef USE_GPU
        tables[result] = gpu_concatenate(concat_tables);
#else
        tables[result] = arrow::ConcatenateTables(concat_tables).ValueOrDie();
#endif
    }

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
#ifdef USE_GPU
        GpuDatum result = traverse_ast(cmd);
        tables[table_name] = it->second->SetColumn(field_name, std::move(result.col));
#else
        arrow::Datum result = traverse_ast(cmd);
        tables[table_name] = set_column(it->second, field_name, result);
#endif
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
#ifdef USE_GPU
        GpuDatum condition = traverse_ast(cmd);
        tables[result_table] = gpu_filter(it->second, condition.col->view());
#else
        arrow::Datum condition = traverse_ast(cmd);
        tables[result_table] = arrow::compute::Filter(it->second, condition).ValueOrDie().table();
#endif
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

void Partition::operation_barrier(char* cmd)
{
    CkCallback cb(CkReductionTarget(Aggregator, barrier_handler), agg_proxy[0]);
    contribute(sizeof(int), &EPOCH, CkReduction::min_int, cb);
    complete_operation();
}

void Partition::operation_reduction(char* cmd)
{
    int table_name = extract<int>(cmd);
    int col_size = extract<int>(cmd);
    std::string col_name(cmd, col_size);
    cmd += col_size;
    AggregateOperation op = static_cast<AggregateOperation>(extract<int>(cmd));
    TablePtr table = get_table(table_name);

#ifdef USE_GPU
    // GPU reduction: cudf::reduce on device column, extract scalar to host for Charm++ contribute
    cudf::column_view col = table->GetColumnByName(col_name);
    auto gpu_scalar = gpu_reduce(col, op);

    // Extract scalar value to host and contribute via Charm++ reduction
    auto col_type = col.type().id();
    if (col_type == cudf::type_id::INT32)
    {
        auto val = static_cast<cudf::numeric_scalar<int32_t>*>(gpu_scalar.get())->value();
        ScalarPtr arrow_scalar = std::make_shared<arrow::Int32Scalar>(val);
        reduce_scalar<int>(arrow_scalar, op);
    }
    else if (col_type == cudf::type_id::INT64 ||
             col_type == cudf::type_id::TIMESTAMP_NANOSECONDS)
    {
        auto val = static_cast<cudf::numeric_scalar<int64_t>*>(gpu_scalar.get())->value();
        ScalarPtr arrow_scalar = std::make_shared<arrow::Int64Scalar>(val);
        reduce_scalar<int64_t>(arrow_scalar, op);
    }
    else if (col_type == cudf::type_id::FLOAT32)
    {
        auto val = static_cast<cudf::numeric_scalar<float>*>(gpu_scalar.get())->value();
        ScalarPtr arrow_scalar = std::make_shared<arrow::FloatScalar>(val);
        reduce_scalar<float>(arrow_scalar, op);
    }
    else if (col_type == cudf::type_id::FLOAT64)
    {
        auto val = static_cast<cudf::numeric_scalar<double>*>(gpu_scalar.get())->value();
        ScalarPtr arrow_scalar = std::make_shared<arrow::DoubleScalar>(val);
        reduce_scalar<double>(arrow_scalar, op);
    }
#else
    ScalarPtr result = local_reduction(table, col_name, op);

    switch (result->type->id())
    {
        case arrow::Type::INT32:
        {
            reduce_scalar<int>(result, op);
            break;
        }

        case arrow::Type::INT64:
        case arrow::Type::TIMESTAMP:
        {
            reduce_scalar<int64_t>(result, op);
            break;
        }

        case arrow::Type::FLOAT:
        {
            reduce_scalar<float>(result, op);
            break;
        }

        case arrow::Type::DOUBLE:
        {
            reduce_scalar<double>(result, op);
            break;
        }
    }
#endif

    complete_operation();
}

#ifndef USE_GPU
ScalarPtr Partition::local_reduction(TablePtr &table, std::string &col_name, AggregateOperation &op)
{
    std::string oper = get_compute_function(op);
    ChunkedArrayPtr col_array = table->GetColumnByName(col_name);
    arrow::Datum result = arrow::compute::CallFunction(oper, {col_array}).ValueOrDie();
    return result.scalar();
}
#endif

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

    LBTurnInstrumentOn();

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
            agg_proxy.ckLocalBranch()->clear_local_chares();
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

        case Operation::Barrier:
        {
            operation_barrier(cmd);
            break;
        }

        case Operation::Reduction:
        {
            operation_reduction(cmd);
            break;
        }

        default:
            break;
    }

    //CkPrintf("Chare %i> Memory usage = %f MB\n", thisIndex, ((double) calculate_memory_usage()) / (1024 * 1024));
}

#ifdef USE_GPU

GpuDatum Partition::extract_operand(char* &msg)
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
            // Copy column view into an owning column
            cudf::column_view col_view = it->second->GetColumnByName(field_name);
            auto col = std::make_unique<cudf::column>(col_view);
            return GpuDatum::from_column(std::move(col));
        }

        case OperandType::Integer:
        {
            int value = extract<int>(msg);
            auto scalar = cudf::make_fixed_width_scalar<int64_t>(static_cast<int64_t>(value));
            scalar->set_valid_async(true);
            return GpuDatum::from_scalar(std::move(scalar));
        }

        case OperandType::Double:
        {
            double value = extract<double>(msg);
            auto scalar = cudf::make_fixed_width_scalar<double>(value);
            scalar->set_valid_async(true);
            return GpuDatum::from_scalar(std::move(scalar));
        }

        case OperandType::Timestamp:
        {
            int64_t value = extract<int64_t>(msg);
            auto scalar = cudf::make_timestamp_scalar<cudf::timestamp_ns>(value);
            scalar->set_valid_async(true);
            return GpuDatum::from_scalar(std::move(scalar));
        }

        default:
            return GpuDatum();
    }
}

GpuDatum Partition::traverse_ast(char* &msg)
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
            GpuDatum lhs = traverse_ast(msg);
            GpuDatum rhs = traverse_ast(msg);
            auto result_col = execute_operation_gpu(lhs, rhs, opcode);
            return GpuDatum::from_column(std::move(result_col));
        }

        default:
            return GpuDatum();
    }
}

#else // !USE_GPU

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

        case OperandType::Timestamp:
        {
            int64_t value = extract<int64_t>(msg);
            ScalarPtr scalar = std::make_shared<arrow::TimestampScalar>(value, arrow::timestamp(arrow::TimeUnit::NANO));
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

#endif // USE_GPU

void Partition::read_parquet(int table_name, std::string file_path)
{
#ifdef USE_GPU
    // GPU path: read parquet directly to GPU memory via cudf
    std::vector<std::string> files = get_matching_files(file_path);

    if (thisIndex == 0)
        CkPrintf("[%d] Reading %lu files (GPU)\n", thisIndex, files.size());

    TablePtr read_tables = nullptr;

    for (int i = 0; i < files.size(); i++)
    {
        std::string file = files[i];

        // Get row count from parquet metadata to compute partition range
        int64_t num_rows = gpu_parquet_num_rows(file);
        int64_t nrows_per_partition = num_rows / num_partitions;
        int64_t start_row = nrows_per_partition * thisIndex;
        int64_t nextra_rows = num_rows - num_partitions * nrows_per_partition;

        if (thisIndex < nextra_rows)
        {
            start_row += thisIndex;
            nrows_per_partition++;
        }
        else
        {
            start_row += nextra_rows;
        }

        auto file_table = gpu_read_parquet(file, start_row, nrows_per_partition);

        if (read_tables == nullptr)
            read_tables = file_table;
        else
        {
            std::vector<TablePtr> to_concat = {read_tables, file_table};
            read_tables = gpu_concatenate(to_concat);
        }
    }

    // Add metadata columns and store
    tables[table_name] = gpu_add_metadata(read_tables, thisIndex);

#else
    // CPU path: read parquet via Arrow
    std::vector<std::string> files = get_matching_files(file_path);
    std::shared_ptr<arrow::io::ReadableFile> input_file;
    TablePtr read_tables = nullptr;

    if (thisIndex == 0)
        CkPrintf("[%d] Reading %i files\n", thisIndex, files.size());

    for (int i = 0; i < files.size(); i++)
    {
        std::string file = files[i];
        input_file = arrow::io::ReadableFile::Open(file).ValueOrDie();

        // Create a ParquetFileReader instance
        std::unique_ptr<parquet::arrow::FileReader> reader;
        parquet::arrow::OpenFile(input_file, arrow::default_memory_pool(), &reader);

        // Get the file metadata
        std::shared_ptr<parquet::FileMetaData> file_metadata = reader->parquet_reader()->metadata();

        int num_rows = file_metadata->num_rows();

        //if (thisIndex == 0)
        //    CkPrintf("[%d] Reading %i rows from %s\n", thisIndex, num_rows, file.c_str());

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
            rows_read += sliced_table->num_rows();
            rows_to_read -= sliced_table->num_rows();
            start_row = 0;  // Reset start_row for subsequent row groups
        }

        TablePtr combined = arrow::ConcatenateTables(row_tables).ValueOrDie();

        if (read_tables == nullptr)
            read_tables = combined;
        else
            read_tables = arrow::ConcatenateTables({read_tables, combined}).ValueOrDie();
    }

    arrow::Int32Builder builder;
    builder.Reserve(read_tables->num_rows());

    for (int32_t i = 0; i < read_tables->num_rows(); ++i)
        builder.Append(i);

    ArrayPtr index_array;
    builder.Finish(&index_array);

    read_tables = set_column(read_tables, "local_index", arrow::Datum(
        arrow::ChunkedArray::Make({index_array}).ValueOrDie()));
    ArrayPtr home_partition_array = arrow::MakeArrayFromScalar(
        *std::make_shared<arrow::Int32Scalar>(thisIndex), 
        read_tables->num_rows()).ValueOrDie();
    tables[table_name] = set_column(read_tables, "home_partition", arrow::Datum(
        arrow::ChunkedArray::Make({home_partition_array}).ValueOrDie()))->CombineChunks().ValueOrDie();

    //CkPrintf("[%d] Read number of rows = %i\n", thisIndex, combined->num_rows());
#endif // !USE_GPU (closing the #else from GPU read_parquet path)
}

Aggregator::Aggregator(CProxy_Main main_proxy_)
        : main_proxy(main_proxy_)
        , num_local_chares(0)
        , join_opts(nullptr)
        , groupby_opts(nullptr)
        , redist_odf(8)
        , expected_rows(0)
        , EPOCH(0)
        , next_temp_name(0)
        , agg_samples_collected(0)
        , sort_tables_collected(0)
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
        , num_local_chares(0)
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
    p | num_partitions;
    p | partition_proxy;
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

void Aggregator::clear_local_chares()
{
    num_local_chares = 0;
    // FIXME this is inefficient, but probably okay
    local_chares.clear();
}

void Aggregator::register_local_chare(int index)
{
    num_local_chares++;
    local_chares.push_back(index);
}

void Aggregator::gather_table(GatherTableDataMsg* msg)
{
    //TablePtr table = deserialize(data, size);
    auto it = gather_count.find(msg->epoch);
    if (it == gather_count.end())
    {
        gather_count[msg->epoch] = 1;
        gather_buffer[msg->epoch].resize(msg->num_partitions);
    }
    else
    {
        gather_count[msg->epoch]++;
    }

    gather_buffer[msg->epoch][msg->chareIdx] = msg;

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
        // table is gathered, send to server and reply ccs
#ifdef USE_GPU
        auto combined_table = gpu_concatenate(gathered_tables);
#else
        auto combined_table = arrow::ConcatenateTables(gathered_tables).ValueOrDie();
#endif
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

//template<typename T>
void Aggregator::reduction_result_int(int result, int epoch)
{
    CkAssert(CkMyPe() == 0);
    char* ret = (char*) malloc(sizeof(int) + sizeof(int));
    int type = arrow::Type::INT32;
    std::memcpy(ret, &type, sizeof(int));
    std::memcpy(ret + sizeof(int), &result, sizeof(int));
    CcsSendDelayedReply(fetch_reply[epoch], sizeof(int) + sizeof(int), ret);
}

void Aggregator::reduction_result_long(int64_t result, int epoch)
{
    CkAssert(CkMyPe() == 0);
    char* ret = (char*) malloc(sizeof(int) + sizeof(int64_t));
    int type = arrow::Type::INT64;
    std::memcpy(ret, &type, sizeof(int));
    std::memcpy(ret + sizeof(int), &result, sizeof(int64_t));
    CcsSendDelayedReply(fetch_reply[epoch], sizeof(int) + sizeof(int64_t), ret);
}

void Aggregator::reduction_result_float(float result, int epoch)
{
    CkAssert(CkMyPe() == 0);
    char* ret = (char*) malloc(sizeof(int) + sizeof(float));
    int type = arrow::Type::FLOAT;
    std::memcpy(ret, &type, sizeof(int));
    std::memcpy(ret + sizeof(int), &result, sizeof(float));
    CcsSendDelayedReply(fetch_reply[epoch], sizeof(int) + sizeof(float), ret);
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
            {
#ifdef USE_GPU
                std::vector<TablePtr> to_concat = {local_table, part_table};
                local_table = gpu_concatenate(to_concat);
#else
                local_table = arrow::ConcatenateTables({local_table, part_table}).ValueOrDie();
#endif
            }
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

#ifdef USE_GPU
    // Extract key names and aggregation info for GPU
    std::vector<std::string> key_names;
    for (auto const& key : agg_opts->keys)
        key_names.push_back(*key.name());

    std::vector<std::pair<AggregateOperation, std::string>> aggregations;
    std::vector<std::string> result_col_names;
    for (auto const& agg : agg_opts->aggregates)
    {
        // Map hash_* function names back to AggregateOperation enum
        AggregateOperation agg_op = AggregateOperation::Sum;
        if (agg.function == "hash_sum") agg_op = AggregateOperation::Sum;
        else if (agg.function == "hash_count") agg_op = AggregateOperation::Count;
        else if (agg.function == "hash_all") agg_op = AggregateOperation::All;
        else if (agg.function == "hash_any") agg_op = AggregateOperation::Any;

        std::string target_col;
        if (!agg.target.empty())
            target_col = *agg.target[0].name();
        aggregations.push_back({agg_op, target_col});
        result_col_names.push_back(agg.name);
    }

    groupby_opts = new GroupByOptions(table_name, result_name,
        key_names, aggregations, result_col_names);

    TablePtr local_table = get_local_table(table_name);
    tables[table_name] = local_table;
    tables[TEMP_TABLE_OFFSET + next_temp_name] = local_table;

    // GPU groupby uses cudf::groupby — two-level aggregation handled in callback
    auto redist_keys = std::vector<std::vector<std::string>>{key_names};
    auto table_names_vec = std::vector<int>{TEMP_TABLE_OFFSET + next_temp_name};
    redistribute(table_names_vec, redist_keys, RedistOperation::GroupBy);

    delete agg_opts;
#else
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
    auto table_names_vec = std::vector<int>{TEMP_TABLE_OFFSET + next_temp_name};
    redistribute(table_names_vec, redist_keys, RedistOperation::GroupBy);
#endif
}

void Aggregator::operation_join(char* cmd)
{
    int table1 = extract<int>(cmd);
    int table2 = extract<int>(cmd);
    int result_name = extract<int>(cmd);
    int nkeys = extract<int>(cmd);

    std::vector<std::string> left_key_names, right_key_names;
#ifndef USE_GPU
    std::vector<arrow::FieldRef> left_keys, right_keys;
#endif

    for (int i = 0; i < nkeys; i++)
    {
        int lkey_size = extract<int>(cmd);
        std::string lkey_name(cmd, lkey_size);
        left_key_names.push_back(lkey_name);
#ifndef USE_GPU
        left_keys.push_back(arrow::FieldRef(lkey_name));
#endif
        cmd += lkey_size;

        int rkey_size = extract<int>(cmd);
        std::string rkey_name(cmd, rkey_size);
        right_key_names.push_back(rkey_name);
#ifndef USE_GPU
        right_keys.push_back(arrow::FieldRef(rkey_name));
#endif
        cmd += rkey_size;
    }

    arrow::acero::JoinType type = static_cast<arrow::acero::JoinType>(extract<int>(cmd));

#ifdef USE_GPU
    join_opts = new JoinOptions(table1, table2, result_name,
        left_key_names, right_key_names, type);
#else
    arrow::acero::HashJoinNodeOptions* opts = new arrow::acero::HashJoinNodeOptions(
        type, left_keys, right_keys, arrow::compute::literal(true),
        "_l", "_r"
    );
    join_opts = new JoinOptions(table1, table2, result_name, opts);
#endif

    start_join();
}

void Aggregator::operation_sort_values(char* cmd)
{
    int table_name = extract<int>(cmd);
    int result_name = extract<int>(cmd);
    int nkeys = extract<int>(cmd);
    std::vector<std::string> keys;

    for (int i = 0; i < nkeys; i++)
    {
        int key_size = extract<int>(cmd);
        keys.push_back(std::string(cmd, key_size));
        cmd += key_size;
    }

    bool ascending = extract<bool>(cmd);

#ifdef USE_GPU
    // Sort local data on GPU
    tables[table_name] = get_local_table(table_name);

    std::vector<std::string> sort_key_names;
    std::vector<cudf::order> sort_orders;
    for (const std::string& key : keys)
    {
        sort_key_names.push_back(key);
        sort_orders.push_back(ascending ? cudf::order::ASCENDING : cudf::order::DESCENDING);
    }

    tables[table_name] = gpu_sort(tables[table_name], sort_key_names, sort_orders);
    sort_values_opts = new SortValuesOptions(table_name, result_name, sort_key_names, sort_orders);

    // Extract samples by converting sort column to Arrow (small D2H copy for splitter values)
    auto arrow_table = tables[table_name]->to_arrow();
    auto column = arrow_table->GetColumnByName(keys[0]);
    int num_samples = CkNumPes() - 1;
    std::vector<int64_t> samples(num_samples);
    for (int i = 0; i < num_samples; i++)
    {
        int index = (i + 1) * column->length() / CkNumPes();
        auto scalar = column->GetScalar(index).ValueOrDie();
        switch (scalar->type->id())
        {
            case arrow::Type::INT64:
                samples[i] = std::dynamic_pointer_cast<arrow::Int64Scalar>(scalar)->value;
                break;
            case arrow::Type::TIMESTAMP:
                samples[i] = std::dynamic_pointer_cast<arrow::TimestampScalar>(scalar)->value;
                break;
            case arrow::Type::INT32:
                samples[i] = (int64_t) std::dynamic_pointer_cast<arrow::Int32Scalar>(scalar)->value;
                break;
            case arrow::Type::DOUBLE:
                samples[i] = (int64_t) std::dynamic_pointer_cast<arrow::DoubleScalar>(scalar)->value;
                break;
            default:
                CkAbort("Sort: unsupported column type for sample extraction");
        }
    }

    thisProxy[0].collect_samples(num_samples, samples.data());

#else
    std::vector<arrow::compute::SortKey> sort_keys;
    for (const std::string& key : keys)
    {
        sort_keys.push_back(
            arrow::compute::SortKey(
                arrow::FieldRef(key),
                ascending ? arrow::compute::SortOrder::Ascending : arrow::compute::SortOrder::Descending
            )
        );
    }

    // Sort local data.
    tables[table_name] = get_local_table(table_name);
    auto indices_result = arrow::compute::SortIndices(arrow::Datum(tables[table_name]), arrow::compute::SortOptions(sort_keys)).ValueOrDie();
    auto sorted_table = arrow::compute::Take(tables[table_name], indices_result).ValueOrDie().table();

    // Get samples from sorted table.
    auto column = sorted_table->GetColumnByName(keys[0]);
    auto sort_column_type = column->type()->id();
    sort_values_opts = new SortValuesOptions(table_name, result_name, sort_keys, sort_column_type);

    int num_samples = CkNumPes() - 1;
    std::vector<int64_t> samples(num_samples);
    for (int i = 0; i < num_samples; i++)
    {
        int index = (i + 1) * column->length() / CkNumPes();
        switch (sort_column_type)
        {
            case arrow::Type::INT64:
                samples[i] = std::dynamic_pointer_cast<arrow::Int64Scalar>(column->GetScalar(index).ValueOrDie())->value;
                break;
            case arrow::Type::TIMESTAMP:
                samples[i] = std::dynamic_pointer_cast<arrow::TimestampScalar>(column->GetScalar(index).ValueOrDie())->value;
                break;
            case arrow::Type::INT32:
                samples[i] = (int64_t) std::dynamic_pointer_cast<arrow::Int32Scalar>(column->GetScalar(index).ValueOrDie())->value;
                break;
            case arrow::Type::DOUBLE:
                samples[i] = (int64_t) std::dynamic_pointer_cast<arrow::DoubleScalar>(column->GetScalar(index).ValueOrDie())->value;
                break;
            default:
                CkAbort("Sort: unsupported column type for sample extraction");
        }
    }

    thisProxy[0].collect_samples(num_samples, samples.data());
#endif
}

void Aggregator::collect_samples(int num_samples, int64_t samples[num_samples])
{
    CkAssert(CkMyPe() == 0);
    
    for (int i = 0; i < num_samples; i++)
    {
        all_samples.push_back(samples[i]);
    }

    if (++agg_samples_collected == CkNumPes())
    {
        // Sort samples to find splitters.
        std::sort(all_samples.begin(), all_samples.end());

        std::vector<int64_t> splitters(CkNumPes() - 1);
        for (int i = 0; i < CkNumPes() - 1; i++)
        {
            splitters[i] = all_samples[(i + 1) * all_samples.size() / CkNumPes()];
        }

        // Send splitters to all aggregators.
        thisProxy.receive_splitters(splitters.size(), splitters.data());
    }
}

void Aggregator::receive_splitters(int num_splitters, int64_t splitters[num_splitters])
{
    assert(num_splitters == CkNumPes() - 1);

    auto table = tables[sort_values_opts->table_name];

#ifdef USE_GPU
    // GPU: build boolean masks on device for range partitioning
    std::string sort_key = sort_values_opts->sort_key_names[0];
    bool descending = (sort_values_opts->sort_orders[0] == cudf::order::DESCENDING);

    for (int i = 0; i < CkNumPes(); i++)
    {
        cudf::column_view sort_col = table->GetColumnByName(sort_key);
        std::unique_ptr<cudf::column> mask;

        if (i == 0)
        {
            auto splitter_scalar = cudf::make_fixed_width_scalar<int64_t>(splitters[0]);
            splitter_scalar->set_valid_async(true);
            mask = cudf::binary_operation(sort_col, *splitter_scalar,
                cudf::binary_operator::LESS, cudf::data_type{cudf::type_id::BOOL8});
        }
        else if (i == CkNumPes() - 1)
        {
            auto splitter_scalar = cudf::make_fixed_width_scalar<int64_t>(splitters[i - 1]);
            splitter_scalar->set_valid_async(true);
            mask = cudf::binary_operation(sort_col, *splitter_scalar,
                cudf::binary_operator::GREATER_EQUAL, cudf::data_type{cudf::type_id::BOOL8});
        }
        else
        {
            auto lo_scalar = cudf::make_fixed_width_scalar<int64_t>(splitters[i - 1]);
            lo_scalar->set_valid_async(true);
            auto hi_scalar = cudf::make_fixed_width_scalar<int64_t>(splitters[i]);
            hi_scalar->set_valid_async(true);
            auto ge_mask = cudf::binary_operation(sort_col, *lo_scalar,
                cudf::binary_operator::GREATER_EQUAL, cudf::data_type{cudf::type_id::BOOL8});
            auto lt_mask = cudf::binary_operation(sort_col, *hi_scalar,
                cudf::binary_operator::LESS, cudf::data_type{cudf::type_id::BOOL8});
            mask = cudf::binary_operation(ge_mask->view(), lt_mask->view(),
                cudf::binary_operator::BITWISE_AND, cudf::data_type{cudf::type_id::BOOL8});
        }

        auto filtered_table = gpu_filter(table, mask->view());

        BufferPtr out;
        serialize(filtered_table, out);
        SortTableMsg* msg = new (out->size()) SortTableMsg(EPOCH, out->size());
        std::memcpy(msg->data, out->data(), out->size());

        int receiver_idx = descending ? (CkNumPes() - 1 - i) : i;
        thisProxy[receiver_idx].receive_sort_tables(msg);
    }

#else
    // Create a typed literal from a splitter value based on the sort column type.
    auto make_splitter_literal = [this](int64_t value) -> arrow::Expression {
        switch (sort_values_opts->sort_column_type)
        {
            case arrow::Type::TIMESTAMP:
                return arrow::compute::literal(
                    std::make_shared<arrow::TimestampScalar>(value, arrow::timestamp(arrow::TimeUnit::NANO)));
            default:
                return arrow::compute::literal(value);
        }
    };

    // For each PE, create a filter for the table based on the splitters.
    for (int i = 0; i < CkNumPes(); i++) {
        arrow::Expression mask;
        arrow::Expression column_ref = arrow::compute::field_ref(sort_values_opts->sort_keys[0].target);
        if (i == 0)
            mask = arrow::compute::less(column_ref, make_splitter_literal(splitters[0]));
        else if (i == CkNumPes() - 1)
            mask = arrow::compute::greater_equal(column_ref, make_splitter_literal(splitters[i - 1]));
        else
            mask = arrow::compute::and_(
                arrow::compute::greater_equal(column_ref, make_splitter_literal(splitters[i - 1])),
                arrow::compute::less(column_ref, make_splitter_literal(splitters[i]))
            );

        arrow::acero::Declaration source{"table_source", arrow::acero::TableSourceNodeOptions{table}};
        arrow::acero::Declaration filter{"filter", {source}, arrow::acero::FilterNodeOptions{mask}};
        auto filtered_table = arrow::acero::DeclarationToTable(std::move(filter)).ValueOrDie();

        BufferPtr out;
        serialize(filtered_table, out);
        SortTableMsg* msg = new (out->size()) SortTableMsg(EPOCH, out->size());
        std::memcpy(msg->data, out->data(), out->size());

        // If sorting descending order, send to the last PE first.
        int receiver_idx = (sort_values_opts->sort_keys[0].order == arrow::compute::SortOrder::Descending)
             ? (CkNumPes() - 1 - i)
             : i;

        thisProxy[receiver_idx].receive_sort_tables(msg);
    }
#endif
}

void Aggregator::receive_sort_tables(SortTableMsg* msg)
{
    auto received_table = deserialize(msg->data, msg->size);

    auto it = tables.find(TEMP_TABLE_OFFSET + next_temp_name);
    if (it == std::end(tables))
        tables[TEMP_TABLE_OFFSET + next_temp_name] = received_table;
    else
    {
#ifdef USE_GPU
        std::vector<TablePtr> to_concat = {it->second, received_table};
        it->second = gpu_concatenate(to_concat);
#else
        it->second = arrow::ConcatenateTables({it->second, received_table}).ValueOrDie();
#endif
    }

    if (++sort_tables_collected == CkNumPes())
    {
#ifdef USE_GPU
        tables[TEMP_TABLE_OFFSET + next_temp_name] = gpu_sort(
            tables[TEMP_TABLE_OFFSET + next_temp_name],
            sort_values_opts->sort_key_names, sort_values_opts->sort_orders);
#else
        auto indices_result = arrow::compute::SortIndices(arrow::Datum(tables[TEMP_TABLE_OFFSET + next_temp_name]), arrow::compute::SortOptions(sort_values_opts->sort_keys)).ValueOrDie();
        tables[TEMP_TABLE_OFFSET + next_temp_name] = arrow::compute::Take(tables[TEMP_TABLE_OFFSET + next_temp_name], indices_result).ValueOrDie().table();
#endif
        partition_table(tables[TEMP_TABLE_OFFSET + next_temp_name], sort_values_opts->result_name);
        complete_sort_values();
    }
}

void Aggregator::barrier_handler(int epoch)
{
    CcsSendDelayedReply(fetch_reply[epoch], 0, NULL);
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

        case Operation::SortValues:
        {
            operation_sort_values(cmd);
            break;
        }

        default:
            break;
    }
}

#ifdef USE_GPU

void Aggregator::update_histogram(TablePtr table, std::vector<int> &hist)
{
    // GPU: gather hash column to host for histogram computation
    cudf::column_view key_col = table->GetColumnByName("_mapped_key");
    auto arrow_table = table->to_arrow();
    auto key_array = arrow_table->GetColumnByName("_mapped_key");
    for (int i = 0; i < key_array->length(); i++)
    {
        uint32_t key = std::dynamic_pointer_cast<arrow::UInt32Scalar>(key_array->GetScalar(i).ValueOrDie())->value;
        hist[key]++;
    }
}

TablePtr Aggregator::map_keys(TablePtr &table, std::vector<std::string> &fields)
{
    // GPU: use cudf::hashing::murmurhash3 for column-level hashing
    std::vector<cudf::column_view> key_views;
    for (auto const& field : fields)
        key_views.push_back(table->GetColumnByName(field));

    cudf::table_view key_table(key_views);
    auto hash_col = cudf::hashing::murmurhash3_x86_32(key_table);

    // Apply modulo to map to buckets: hash % (redist_odf * CkNumPes())
    int32_t num_buckets = redist_odf * CkNumPes();
    auto mod_scalar = cudf::make_fixed_width_scalar<int32_t>(num_buckets);
    mod_scalar->set_valid_async(true);
    auto mapped_col = cudf::binary_operation(
        hash_col->view(), *mod_scalar,
        cudf::binary_operator::PYMOD,
        cudf::data_type{cudf::type_id::UINT32});

    return table->SetColumn("_mapped_key", std::move(mapped_col));
}

void Aggregator::redistribute(std::vector<int> table_names, std::vector<std::vector<std::string>> &keys,
    RedistOperation oper)
{
    redist_table_names = table_names;
    redist_operation = oper;
    for (int i = 0; i < table_names.size(); i++)
    {
        if (tables[table_names[i]] != nullptr && tables[table_names[i]]->num_rows() > 0)
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

#else // !USE_GPU

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
            switch (arr->type()->id())
            {
                case arrow::Type::STRING:
                {
                    std::string key = std::dynamic_pointer_cast<arrow::StringScalar>(
                        arr->GetScalar(i).ValueOrDie())->value->ToString();
                    XXH32_update(hash_state, key.c_str(), key.size());
                    break;
                }

                case arrow::Type::INT32:
                {
                    int32_t key = std::dynamic_pointer_cast<arrow::Int32Scalar>(
                        arr->GetScalar(i).ValueOrDie())->value;
                    XXH32_update(hash_state, &key, sizeof(int32_t));
                    break;
                }

                case arrow::Type::INT64:
                {
                    int64_t key = std::dynamic_pointer_cast<arrow::Int64Scalar>(
                        arr->GetScalar(i).ValueOrDie())->value;
                    XXH32_update(hash_state, &key, sizeof(int64_t));
                    break;
                }

                case arrow::Type::FLOAT:
                {
                    float key = std::dynamic_pointer_cast<arrow::FloatScalar>(
                        arr->GetScalar(i).ValueOrDie())->value;
                    XXH32_update(hash_state, &key, sizeof(float));
                    break;
                }

                case arrow::Type::DOUBLE:
                {
                    double key = std::dynamic_pointer_cast<arrow::DoubleScalar>(
                        arr->GetScalar(i).ValueOrDie())->value;
                    XXH32_update(hash_state, &key, sizeof(double));
                    break;
                }

                case arrow::Type::TIMESTAMP:
                {
                    int64_t key = std::dynamic_pointer_cast<arrow::TimestampScalar>(
                        arr->GetScalar(i).ValueOrDie())->value;
                    XXH32_update(hash_state, &key, sizeof(int64_t));
                    break;
                }

                default:
                {
                    CkAbort("Not implemented shuffling on this key type yet!");
                }
            }
        }

        XXH32_hash_t final_hash = XXH32_digest(hash_state);

        builder.Append(static_cast<uint32_t>(final_hash) % (redist_odf * CkNumPes()));
    }

    builder.Finish(&hash_array);
    return set_column(table, "_mapped_key", arrow::Datum(arrow::ChunkedArray::Make({hash_array}).ValueOrDie()));
}

#endif // USE_GPU

void Aggregator::start_join()
{
    tables[join_opts->table1] = get_local_table(join_opts->table1);
    tables[join_opts->table2] = get_local_table(join_opts->table2);

#ifdef USE_GPU
    auto redist_keys = std::vector<std::vector<std::string>>{
        join_opts->left_keys, join_opts->right_keys};
#else
    auto redist_keys = std::vector<std::vector<arrow::FieldRef>>{
        join_opts->opts->left_keys, join_opts->opts->right_keys};
#endif
    redistribute({join_opts->table1, join_opts->table2},
        redist_keys, RedistOperation::Join);
}

#ifndef USE_GPU
void Aggregator::redistribute(std::vector<int> table_names, std::vector<std::vector<arrow::FieldRef>> &keys,
    RedistOperation oper)
{
    redist_table_names = table_names;
    redist_operation = oper;
    for (int i = 0; i < table_names.size(); i++)
    {
        if (tables[table_names[i]] != nullptr && tables[table_names[i]]->num_rows() > 0)
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
#endif // !USE_GPU

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

    /*CkPrintf("Loads after reshuffling:\n");
    for (int i = 0; i < CkNumPes(); i++)
        CkPrintf("%i, ", expected_loads[i]);
    CkPrintf("\n");*/

    thisProxy.shuffle_data(pe_map, expected_loads);
}

void Aggregator::shuffle_data(std::vector<int> pe_map, std::vector<int> expected_loads)
{
    expected_rows = expected_loads[CkMyPe()];

    std::vector<BufferPtr> serialized_buffers[CkNumPes()];
    int total_size[CkNumPes()];
    for (int i = 0; i < CkNumPes(); i++)
        total_size[i] = 0;

    for (int i = 0; i < redist_table_names.size(); i++)
    {
        int table_name = redist_table_names[i];
        TablePtr table = tables[table_name];
        std::vector<std::vector<int>> indices(CkNumPes());

#ifdef USE_GPU
        // GPU: transfer mapped key column to host for PE assignment
        auto arrow_table = table->to_arrow();
        auto key_array = arrow_table->GetColumnByName("_mapped_key");
        for (int j = 0; j < key_array->length(); j++)
        {
            int pe = pe_map[std::dynamic_pointer_cast<arrow::UInt32Scalar>(key_array->GetScalar(j).ValueOrDie())->value];
            indices[pe].push_back(j);
        }
#else
        ChunkedArrayPtr key_array = table->GetColumnByName("_mapped_key");
        for (int j = 0; j < key_array->length(); j++)
        {
            int pe = pe_map[std::dynamic_pointer_cast<arrow::UInt32Scalar>(key_array->GetScalar(j).ValueOrDie())->value];
            indices[pe].push_back(j);
        }
#endif

        for (int j = 0; j < CkNumPes(); j++)
        {
            if (indices[j].size() > 0)
            {
#ifdef USE_GPU
                // GPU: build gather map on device and use cudf::gather
                auto indices_col = cudf::make_fixed_width_column(
                    cudf::data_type{cudf::type_id::INT32}, indices[j].size());
                cudaMemcpy(indices_col->mutable_view().data<int32_t>(),
                    indices[j].data(), indices[j].size() * sizeof(int32_t),
                    cudaMemcpyHostToDevice);
                auto gathered = cudf::gather(table->view(), indices_col->view());
                auto selected = std::make_shared<GpuTable>(
                    std::move(gathered), std::vector<std::string>(table->field_names()));
#else
                ChunkedArrayPtr indices_array = array_from_vector(indices[j]);
                TablePtr selected = arrow::compute::Take(table, indices_array).ValueOrDie().table();
#endif
                if (j == CkMyPe())
                {
                    auto it = redist_tables.find(table_name);
                    if (it == std::end(redist_tables))
                        redist_tables[table_name] = selected;
                    else
                    {
#ifdef USE_GPU
                        std::vector<TablePtr> to_concat = {it->second, selected};
                        it->second = gpu_concatenate(to_concat);
#else
                        it->second = arrow::ConcatenateTables({it->second, selected}).ValueOrDie();
#endif
                    }
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
            RedistTableMsg* msg = new (total_size[i], serialized_buffers[i].size()) RedistTableMsg(
                serialized_buffers[i], total_size[i]);
            thisProxy[i].receive_shuffle_data(msg);
        }
    }

    for (int i = 0; i < redist_table_names.size(); i++)
    {
        int table_name = redist_table_names[i];
        tables.erase(table_name);

        for (int j = 0; j < num_local_chares; j++)
        {
            int index = local_chares[j];
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
#ifdef USE_GPU
    if (tables[TEMP_TABLE_OFFSET + next_temp_name] != nullptr && tables[TEMP_TABLE_OFFSET + next_temp_name]->num_rows() > 0)
    {
        TablePtr result = gpu_groupby(tables[TEMP_TABLE_OFFSET + next_temp_name],
            groupby_opts->key_names, groupby_opts->aggregations, groupby_opts->result_names);
        result = gpu_clean_metadata(result);
        partition_table(result, groupby_opts->result_name);
    }
#else
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

    if (tables[TEMP_TABLE_OFFSET + next_temp_name] != nullptr && tables[TEMP_TABLE_OFFSET + next_temp_name]->num_rows() > 0)
    {
        TablePtr result = local_aggregation(tables[TEMP_TABLE_OFFSET + next_temp_name], *groupby_opts->opts);
        result = clean_metadata(result);
        partition_table(result, groupby_opts->result_name);
    }
#endif

    complete_groupby();
}

void Aggregator::join_callback()
{
#ifdef USE_GPU
    // GPU: resolve key column indices for gpu_join
    std::vector<cudf::size_type> left_on, right_on;
    for (auto const& key : join_opts->left_keys)
        left_on.push_back(tables[join_opts->table1]->GetColumnIndex(key));
    for (auto const& key : join_opts->right_keys)
        right_on.push_back(tables[join_opts->table2]->GetColumnIndex(key));

    TablePtr result = gpu_join(tables[join_opts->table1], tables[join_opts->table2],
        left_on, right_on, join_opts->join_type);
    result = gpu_clean_metadata(result);
    partition_table(tables[join_opts->table1], join_opts->table1);
    partition_table(tables[join_opts->table2], join_opts->table2);
    partition_table(result, join_opts->result_name);
#else
    TablePtr result = local_join(tables[join_opts->table1], tables[join_opts->table2], *join_opts->opts);
    result = clean_metadata(result);
    partition_table(tables[join_opts->table1], join_opts->table1);
    partition_table(tables[join_opts->table2], join_opts->table2);
    partition_table(result, join_opts->result_name);
#endif
    complete_join();
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
        {
#ifdef USE_GPU
            std::vector<TablePtr> to_concat = {it->second, remote_tables[i]};
            it->second = gpu_concatenate(to_concat);
#else
            it->second = arrow::ConcatenateTables({it->second, remote_tables[i]}).ValueOrDie();
#endif
        }
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
    for (int i = 0; i < num_local_chares; i++)
    {
        int index = local_chares[i];
        partition_proxy[index].ckLocal()->complete_operation();
    }

    redist_table_names.clear();
    tables.clear();
    redist_tables.clear();

    EPOCH++;
    thisProxy[thisIndex].poll();
}

void Aggregator::complete_groupby()
{
    tables.erase(groupby_opts->table_name);
#ifndef USE_GPU
    delete groupby_opts->opts;
#endif
    delete groupby_opts;
    groupby_opts = nullptr;
    tables.erase(TEMP_TABLE_OFFSET + next_temp_name++);

    complete_operation();
}

void Aggregator::complete_join()
{
    tables.erase(join_opts->table1);
    tables.erase(join_opts->table2);
#ifndef USE_GPU
    delete join_opts->opts;
#endif
    delete join_opts;
    join_opts = nullptr;

    complete_operation();
}

void Aggregator::complete_sort_values()
{
    tables.erase(sort_values_opts->table_name);
    tables.erase(TEMP_TABLE_OFFSET + next_temp_name++);
    delete sort_values_opts;
    sort_values_opts = nullptr;
    agg_samples_collected = 0;
    sort_tables_collected = 0;

    complete_operation();
}

#ifndef USE_GPU
TablePtr Aggregator::local_join(TablePtr &t1, TablePtr &t2, arrow::acero::HashJoinNodeOptions &opts)
{
    arrow::acero::Declaration left{"table_source", arrow::acero::TableSourceNodeOptions(t1)};
    arrow::acero::Declaration right{"table_source", arrow::acero::TableSourceNodeOptions(t2)};

    arrow::acero::Declaration hashjoin{"hashjoin", {std::move(left), std::move(right)}, opts};

    // Collect the results
    return arrow::acero::DeclarationToTable(std::move(hashjoin)).ValueOrDie();
}
#endif

void Aggregator::partition_table(TablePtr table, int result_name)
{
    // partition table into local chares
    int remain_chares = num_local_chares;
    int remain_rows = table->num_rows();
    int offset = 0;
    for (int i = 0; i < num_local_chares; i++)
    {
        if (remain_rows == 0)
            return;
        int index = local_chares[i];
        Partition* partition = partition_proxy[index].ckLocal();
        int rows_per_chare = remain_rows / remain_chares;
        partition->add_table(result_name, table->Slice(offset, rows_per_chare));
        offset += rows_per_chare;
        remain_rows -= rows_per_chare;
        remain_chares--;
    }
}

void Aggregator::start_polling()
{
    CkPrintf("Resume polling\n");
    thisProxy.poll();
    partition_proxy.poll();
}

// GPU-aware message handlers (stubs — used when USE_GPU is defined for RDMA transfers)
void Aggregator::gpu_gather_table(GpuGatherMsg* msg)
{
#ifdef USE_GPU
    auto table = msg->get_table();

    auto it = gpu_gather_buffer.find(msg->epoch);
    if (it == gpu_gather_buffer.end())
        gpu_gather_buffer[msg->epoch] = std::vector<GpuGatherMsg*>();

    gpu_gather_buffer[msg->epoch].push_back(msg);

    // Check if all partitions have reported
    if (static_cast<int>(gpu_gather_buffer[msg->epoch].size()) == num_partitions)
    {
        std::vector<TablePtr> gathered_tables;
        for (auto* m : gpu_gather_buffer[msg->epoch])
        {
            auto t = m->get_table();
            if (t != nullptr && t->num_rows() > 0)
                gathered_tables.push_back(t);
        }

        auto combined_table = gpu_concatenate(gathered_tables);
        CkPrintf("GPU gathered table data with size = %i\n", combined_table->num_rows());
        BufferPtr out;
        serialize(combined_table, out);
        fetch_callback(msg->epoch, out);

        gpu_gather_buffer.erase(msg->epoch);
    }
#endif
}

void Aggregator::gpu_receive_shuffle_data(GpuRedistMsg* msg)
{
#ifdef USE_GPU
    auto remote_tables = msg->get_tables();

    int total_redist_rows = 0;
    for (size_t i = 0; i < remote_tables.size(); i++)
    {
        int table_name = redist_table_names[i];
        auto it = redist_tables.find(table_name);
        if (it == std::end(redist_tables))
            redist_tables[table_name] = remote_tables[i];
        else
        {
            std::vector<TablePtr> to_concat = {it->second, remote_tables[i]};
            it->second = gpu_concatenate(to_concat);
        }
        total_redist_rows += redist_tables[table_name]->num_rows();
    }

    if (expected_rows != -1 && total_redist_rows == expected_rows)
    {
        for (size_t i = 0; i < redist_table_names.size(); i++)
        {
            int table_name = redist_table_names[i];
            tables[table_name] = redist_tables[table_name];
            redist_tables.erase(table_name);
        }
        redist_callback();
    }
#endif
}

void Aggregator::gpu_receive_sort_tables(GpuTableMsg* msg)
{
#ifdef USE_GPU
    auto received_table = msg->get_table();

    auto it = tables.find(TEMP_TABLE_OFFSET + next_temp_name);
    if (it == std::end(tables))
        tables[TEMP_TABLE_OFFSET + next_temp_name] = received_table;
    else
    {
        std::vector<TablePtr> to_concat = {it->second, received_table};
        it->second = gpu_concatenate(to_concat);
    }

    if (++sort_tables_collected == CkNumPes())
    {
        tables[TEMP_TABLE_OFFSET + next_temp_name] = gpu_sort(
            tables[TEMP_TABLE_OFFSET + next_temp_name],
            sort_values_opts->sort_key_names, sort_values_opts->sort_orders);
        partition_table(tables[TEMP_TABLE_OFFSET + next_temp_name], sort_values_opts->result_name);
        complete_sort_values();
    }
#endif
}