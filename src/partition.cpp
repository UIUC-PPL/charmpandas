#include "utils.hpp"
#include "operations.hpp"
#include "messaging.hpp"
#include "reduction.hpp"
#include "serialize.hpp"
#include "partition.hpp"
#include "messaging.def.h"
#include "partition.def.h"

Partition::Partition(int num_partitions_, int lb_period_, CProxy_Aggregator agg_proxy_) 
    : num_partitions(num_partitions_)
    , agg_proxy(agg_proxy_)
    , EPOCH(0)
    , join_count(0)
    , local_join_done(false)
    , lb_period(lb_period_)
{
    usesAtSync = true;

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

inline void Partition::add_table(int table_name, TablePtr table)
{
    tables[table_name] = table;
}

ArrayPtr Partition::array_from_vector(std::vector<int> &indices)
{
    arrow::Int32Builder builder;
    builder.AppendValues(indices.data(), indices.size());
    return builder.Finish().ValueOrDie();
}

void Partition::request_local_table(int table_name, std::vector<int> local_indices, uint8_t dir)
{
    TablePtr table = tables[table_name];
    auto indices_array = array_from_vector(local_indices);
    TablePtr selected = arrow::compute::Take(table, indices_array).ValueOrDie().table();
    agg_proxy.ckLocalBranch()->receive_local_table(selected, dir);
}

void Partition::request_remote_table(int table_name, std::vector<int> local_indices, uint8_t dir,
    int pe_dest)
{
    TablePtr table = tables[table_name];
    auto indices_array = array_from_vector(local_indices);
    TablePtr selected = arrow::compute::Take(table, indices_array).ValueOrDie().table();
    BufferPtr out;
    serialize(selected, out);
    RemoteTableMsg* msg = new (out->size()) RemoteTableMsg(EPOCH, out->size(), dir);
    std::memcpy(msg->data, out->data(), out->size());
    agg_proxy[pe_dest].receive_remote_table(msg);
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

void Partition::aggregate_result(CkReductionMsg* msg)
{
    CkAssert(thisIndex == 0);
    AggregateReductionMsg* agg_msg = (AggregateReductionMsg*) msg->getData();
    arrow::acero::AggregateNodeOptions agg_opts = extract_aggregate_options(agg_msg->get_options());
    CkAssert(agg_msg->table_size != 0);
    tables[agg_msg->result_name] = deserialize(agg_msg->get_table(), agg_msg->table_size);
    complete_operation();
}

void Partition::execute_command(int epoch, int size, char* cmd)
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
    
        default:
            break;
    }

    CkPrintf("Chare %i> Memory usage = %f MB\n", thisIndex, ((double) calculate_memory_usage()) / (1024 * 1024));
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

    auto local_index_array = arrow::compute::CallFunction(
        "enumerate", 
        {combined->column(0)}
    ).ValueOrDie();

    combined = set_column(combined, "local_index", local_index_array);
    tables[table_name] = set_column(combined, "home_partition", arrow::Datum(std::make_shared<arrow::Int32Scalar>(thisIndex)));

    CkPrintf("[%d] Read number of rows = %i\n", thisIndex, combined->num_rows());
}

Aggregator::Aggregator(CProxy_Main main_proxy_)
        : main_proxy(main_proxy_)
        , num_local_chares(-1)
        , remote_buffer_limit(-1)
        , next_local_chare(0)
        , num_sends(0)
        , num_recvs(0)
        , num_active_requests(0)
        , num_expected_tables(0)
        , join_count(0)
        , local_t1(nullptr)
        , local_t2(nullptr)
        , join_opts(nullptr)
        , join_left_tables(nullptr)
        , join_right_tables(nullptr)
        , result_indices(nullptr)
        , EPOCH(0)
{}

Aggregator::Aggregator(CkMigrateMessage* m) : CBase_Aggregator(m) {}

void Aggregator::pup(PUP::er &p) {}

void Aggregator::init_done()
{
    main_proxy.init_done();
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

void Aggregator::fetch_callback(int epoch, BufferPtr &out)
{
    CkAssert(CkMyPe() == 0);
    CcsSendDelayedReply(fetch_reply[epoch], out->size(), (char*) out->data());
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

void Aggregator::execute_command(int epoch, int size, char* cmd)
{
    Operation op = lookup_operation(extract<int>(cmd));

    switch (op)
    {
        case Operation::Join:
        {
            operation_join(cmd);
            break;
        }

        default:
            break;
    }
}

void Aggregator::start_join()
{
    if (CkNumPes() > 1)
    {
        for (int i = 0; i < remote_buffer_limit; i++)
        {
            if (i < num_local_chares)
            {
                send_local_data();
                ++num_sends;
            }
        }
    }

    std::vector<TablePtr> local_list1, local_list2;

    for (int i = 0; i < num_local_chares; i++)
    {
        int index = local_chares[i];
        Partition* partition = partition_proxy[index].ckLocal();
        std::vector<std::string> fields = {"home_partition", "local_index"};
        TablePtr table1 = partition->get_table(join_opts->table1, fields);
        TablePtr table2 = partition->get_table(join_opts->table2, fields);
        if (table1 != nullptr)
            local_list1.push_back(table1);
        if (table2 != nullptr)
            local_list2.push_back(table2);
    }

    if (local_list1.size() > 0)
        local_t1 = arrow::ConcatenateTables(local_list1).ValueOrDie();
    else
        local_t1 = nullptr;

    if (local_list2.size() > 0)
        local_t2 = arrow::ConcatenateTables(local_list2).ValueOrDie();
    else
        local_t2 = nullptr;

    LocalJoinMsg* msg = new (8*sizeof(int)) LocalJoinMsg(local_t1, local_t2, false);
    *((int*) CkPriorityPtr(msg)) = LOCAL_JOIN_PRIO;
    CkSetQueueing(msg, CK_QUEUEING_IFIFO);
    thisProxy[thisIndex].join(msg);
}

void Aggregator::check_remote_table(TablePtr &table)
{
    auto it = remote_tables[table];
    int fwd_count = remote_msgs[table].first->fwd_count;
    if (it.second && (it.first || fwd_count == CkNumPes() - 1))
    {
        remote_tables.erase(table);
        delete remote_msgs[table].first;
        remote_msgs.erase(table);
        if (num_recvs < num_partitions - num_local_chares && num_recvs >= remote_buffer_limit)
            thisProxy[(thisIndex - 1) % CkNumPes()].request_join_data();
    }
}

TablePtr Aggregator::select_remote()
{
    for (auto it = remote_tables.begin(); it != remote_tables.end(); ++it)
        if (remote_msgs[it->first].first->fwd_count != CkNumPes() - 1)
            return it->first;
    return nullptr;
}

void Aggregator::send_local_data()
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

void Aggregator::request_join_data()
{
    // forward table from buffer or
    // send local chare data is buffer is empty
    // do local joins during the free time

    if (remote_tables.size() > 0)
    {
        TablePtr remote_table = select_remote();
        if (remote_table != nullptr)
        {
            RemoteJoinMsg* fwd_msg = remote_msgs[remote_table].first->copy();
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

void Aggregator::process_remote_indices(RemoteJoinMsg* msg)
{
    // this method should put the remote table
    // in the remote_tables buffer and call
    // entry methods for local joins with the remote data

    // the local joins with remote data should be second highest
    // priority
    if (++num_recvs < num_partitions - num_local_chares)
        thisProxy[thisIndex].listen_remote_table();
    TablePtr table = msg->get_table();
    remote_msgs[table] = std::make_pair(msg, table);
    remote_tables[table] = std::make_pair(false, false);

    if (num_sends++ < remote_buffer_limit || num_active_requests > 0)
    {
        remote_tables[table].first = true;
        RemoteJoinMsg* fwd_msg = msg->copy();
        thisProxy[(thisIndex + 1) % CkNumPes()].receive_remote_indices(fwd_msg);
        --num_active_requests;
    }

    // allocate msg and assign priority
    LocalJoinMsg* local_msg = new (8*sizeof(int)) LocalJoinMsg(local_t1, table, true);
    *((int*) CkPriorityPtr(local_msg)) = REMOTE_JOIN_PRIO;
    CkSetQueueing(local_msg, CK_QUEUEING_IFIFO);
    thisProxy[thisIndex].join(local_msg);
}

void Aggregator::send_table_requests(ChunkedArrayPtr partitions, ChunkedArrayPtr indices, uint8_t dir)
{
    int table_name;
    if (dir == LEFT)
        table_name = join_opts->table1;
    else
        table_name = join_opts->table2;
    int last_partition = -1;
    std::vector<int> local_indices;
    for (int i = 0; i < partitions->length(); i++)
    {
        int part = std::dynamic_pointer_cast<arrow::Int32Scalar>(partitions->GetScalar(i).ValueOrDie())->value;
        if (part != last_partition)
        {
            if (last_partition != -1)
            {
                ++num_expected_tables;
                if (partition_proxy[part].ckLocal() != nullptr)
                    // this can be made faster by avoiding local_indices pup
                    partition_proxy[last_partition].request_local_table(table_name, local_indices, dir);
                else
                    partition_proxy[last_partition].request_remote_table(table_name, local_indices, thisIndex, dir);
            }
            local_indices.clear();
            last_partition = part;
        }
        local_indices.push_back(std::dynamic_pointer_cast<arrow::Int32Scalar>(indices->GetScalar(i).ValueOrDie())->value);
    }
}

void Aggregator::fetch_joined_table(TablePtr joined_table)
{
    // fetch rest of the columns of the joined table
    // first do right, left are local to this PE

    std::vector<int> right_keys;
    right_keys.push_back(joined_table->schema()->GetFieldIndex("home_partition_r"));
    right_keys.push_back(joined_table->schema()->GetFieldIndex("local_index_r"));

    TablePtr right_table = joined_table->SelectColumns(right_keys).ValueOrDie();
    right_table = sort_table(right_table, "home_partition_r");
    send_table_requests(right_table->GetColumnByName("home_partition_r"),
        right_table->GetColumnByName("local_index_r"), RIGHT);

    std::vector<int> left_keys;
    left_keys.push_back(joined_table->schema()->GetFieldIndex("home_partition_l"));
    left_keys.push_back(joined_table->schema()->GetFieldIndex("local_index_l"));

    TablePtr left_table = joined_table->SelectColumns(left_keys).ValueOrDie();
    left_table = sort_table(left_table, "home_partition_l");
    send_table_requests(left_table->GetColumnByName("home_partition_l"),
        left_table->GetColumnByName("local_index_l"), LEFT);
}

void Aggregator::complete_operation()
{
    EPOCH++;
    thisProxy[thisIndex].poll();
}

void Aggregator::complete_join()
{
    delete join_opts->opts;
    delete join_opts;
    join_opts = nullptr;
    //EPOCH++;

    num_sends = 0;
    num_recvs = 0;
    join_count = 0;
    local_t1 = nullptr;
    local_t2 = nullptr;
    result_indices = nullptr;

    for (int i = 0; i < num_local_chares; i++)
    {
        int index = local_chares[i];
        partition_proxy[index].ckLocal()->inc_epoch();
    }

    complete_operation();
}

void Aggregator::join(LocalJoinMsg* msg)
{
    TablePtr result = nullptr;
    if (msg->t1 != nullptr && msg->t2 != nullptr)
        result = local_join(msg->t1, msg->t2, *join_opts->opts);
    else if (msg->t1 != nullptr)
        result = msg->t1;
    else if (msg->t2 != nullptr)
        result = msg->t2;
    
    if (result != nullptr)
    {
        fetch_joined_table(result);

        if (result_indices == nullptr)
            result_indices = result;
        else
            result_indices = arrow::ConcatenateTables({result_indices, result}).ValueOrDie();
    }

    join_count += (msg->is_remote ? 1 : num_local_chares);
    
    if (msg->is_remote)
    {
        // set used on local data to true
        remote_tables[msg->t2].second = true;
        // check if this remote table is done
        check_remote_table(msg->t2);
    }
}

void Aggregator::receive_remote_table(RemoteTableMsg* msg)
{
    if (msg->dir == LEFT)
    {
        if (join_left_tables == nullptr)
            join_left_tables = msg->get_table();
        else
            join_left_tables = arrow::ConcatenateTables({join_left_tables, msg->get_table()}).ValueOrDie();
    }
    else
    {
        if (join_right_tables == nullptr)
            join_right_tables = msg->get_table();
        else
            join_right_tables = arrow::ConcatenateTables({join_right_tables, msg->get_table()}).ValueOrDie();
    }

    if (join_count == num_partitions && --num_expected_tables == 0)
    {
        TablePtr result = join_right_left();
        partition_table(result, join_opts->result_name);
        complete_join();
    }
}

void Aggregator::receive_local_table(TablePtr table, uint8_t dir)
{
    if (dir == LEFT)
    {
        if (join_left_tables == nullptr)
            join_left_tables = table;
        else
            join_left_tables = arrow::ConcatenateTables({join_left_tables, table}).ValueOrDie();
    }
    else
    {
        if (join_right_tables == nullptr)
            join_right_tables = table;
        else
            join_right_tables = arrow::ConcatenateTables({join_right_tables, table}).ValueOrDie();
    }

    if (join_count == num_partitions && --num_expected_tables == 0)
    {
        TablePtr result = join_right_left();
        partition_table(result, join_opts->result_name);
        complete_join();
    }
}

TablePtr Aggregator::local_join(TablePtr &t1, TablePtr &t2, arrow::acero::HashJoinNodeOptions &opts)
{
    arrow::acero::Declaration left{"table_source", arrow::acero::TableSourceNodeOptions(t1)};
    arrow::acero::Declaration right{"table_source", arrow::acero::TableSourceNodeOptions(t2)};

    arrow::acero::Declaration hashjoin{"hashjoin", {std::move(left), std::move(right)}, opts};

    // Collect the results
    return arrow::acero::DeclarationToTable(std::move(hashjoin)).ValueOrDie();
}

TablePtr Aggregator::join_right_left()
{
    // left join indices join right
    arrow::acero::HashJoinNodeOptions opts_left{
            arrow::acero::JoinType::INNER, 
            {"home_partition", "local_index"}, {"home_partition_l", "local_index_l"}, 
            arrow::compute::literal(true),
            "_l", "_r"
        };

    TablePtr inter_result = local_join(join_left_tables, result_indices, opts_left);

    arrow::acero::HashJoinNodeOptions opts_right{
            arrow::acero::JoinType::INNER, 
            {"home_partition_r_r", "local_index_r_r"}, {"home_partition", "local_index"},
            arrow::compute::literal(true),
            "", "_r"
        };
    
    TablePtr result = local_join(inter_result, join_right_tables, opts_right);

    //result->RemoveColumn({"home_partition_l_r_l", "local_index_l_r_l", 
    //    "home_partition_r_r_l", "local_index_r_r_l"});

    return result;
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