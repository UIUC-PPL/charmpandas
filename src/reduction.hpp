#ifndef REDUCTION_H
#define REDUCTION_H

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/api.h>
#include <arrow/table.h>
#include <arrow/compute/api.h>
#include "arrow/acero/exec_plan.h"
#include <arrow/acero/options.h>
#include "types.hpp"
#include "utils.hpp"
#include "serialize.hpp"
#include "reduction.decl.h"

CkReduction::reducerType AggregateReductionType;

class AggregateReductionMsg
{
public:
    int result_name;
    int table_size;
    int options_size;

    AggregateReductionMsg(int result_name_, int table_size_, int options_size_)
        : result_name(result_name_)
        , table_size(table_size_)
        , options_size(options_size_)
    {}

    inline char* get_options()
    {
        return reinterpret_cast<char*>((char*) this + sizeof(AggregateReductionMsg));
    }

    inline char* get_table()
    {
        return reinterpret_cast<char*>((char*) this + sizeof(AggregateReductionMsg) + options_size);
    }
};

AggregateReductionMsg* create_reduction_msg(int result_name, int table_size, char* table, 
    int options_size, char* options)
{
    int total_size = sizeof(AggregateReductionMsg) + table_size + options_size;
    void* buffer = ::operator new(total_size);
    AggregateReductionMsg* msg = new (buffer) AggregateReductionMsg(result_name, table_size, options_size);
    char* msg_options = reinterpret_cast<char*>(buffer + sizeof(AggregateReductionMsg));
    if (table_size > 0)
    {
        char* msg_table = reinterpret_cast<char*>(buffer + sizeof(AggregateReductionMsg) + options_size);
        std::memcpy(msg_table, table, table_size);
    }
    std::memcpy(msg_options, options, options_size);
    return msg;
}

enum class AggregateOperation : int
{
    HashSum = 0,
    HashCount = 1,
    HashAll = 2,
    HashAny = 3,
    HashApproximateMedian = 4,
    HashCountDistinct = 5,
    HashDistinct = 6,
    HashFirst = 7,
    HashLast = 8,
    HashFirstLast = 9
};

std::string get_aggregation_function(AggregateOperation op)
{
    switch (op)
    {
        case AggregateOperation::HashSum:
            return "hash_sum";

        case AggregateOperation::HashCount:
            return "hash_count";

        case AggregateOperation::HashAll:
            return "hash_all";

        case AggregateOperation::HashAny:
            return "hash_any";

        case AggregateOperation::HashApproximateMedian:
            return "hash_approximate_median";

        case AggregateOperation::HashCountDistinct:
            return "hash_count_distinct";

        case AggregateOperation::HashDistinct:
            return "hash_distinct";

        case AggregateOperation::HashFirst:
            return "hash_first";

        case AggregateOperation::HashLast:
            return "hash_last";

        case AggregateOperation::HashFirstLast:
            return "hash_first_last";

        default:
            return "error";
    }
}

arrow::acero::AggregateNodeOptions* extract_aggregate_options(char* msg, bool is_local=false)
{
    // first extract the keys
    int nkeys = extract<int>(msg);
    std::vector<arrow::FieldRef> keys;
    for (int i = 0; i < nkeys; i++)
    {
        int key_size = extract<int>(msg);
        std::string key(msg, key_size);
        arrow::FieldRef field(key);
        msg += key_size;
        keys.push_back(field);
    }

    // then extract the aggregations
    std::vector<arrow::compute::Aggregate> aggs;
    int naggs = extract<int>(msg);
    for (int i = 0; i < naggs; i++)
    {
        AggregateOperation agg_op = static_cast<AggregateOperation>(extract<int>(msg));
        std::string agg_fn = get_aggregation_function(agg_op);
        int target_size = extract<int>(msg);
        std::string target_field(msg, target_size);
        msg += target_size;
        int result_size = extract<int>(msg);
        std::string result_field(msg, result_size);
        msg += result_size;
        aggs.push_back({agg_fn, nullptr, is_local ? target_field : result_field, result_field});
    }

    arrow::acero::AggregateNodeOptions* opts = new arrow::acero::AggregateNodeOptions{aggs, keys};
    return opts;
}

bool is_two_level_agg(arrow::acero::AggregateNodeOptions& opts)
{
    for (int i = 0; i < opts.aggregates.size(); i++)
    {
        std::string agg_fn = opts.aggregates[i].function;

        if (agg_fn == "hash_count_distinct")
            return false;

        if (agg_fn == "hash_approximate_median")
            return false;
    }
    return true;
}

std::string aggregation_callback_fn(std::string &agg_fn)
{
    if (agg_fn == "hash_count")
        return "hash_sum";

    if (agg_fn == "hash_sum")
        return "hash_sum";

    return "hash_sum";
}

TablePtr local_aggregation(TablePtr &table, arrow::acero::AggregateNodeOptions &agg_opts)
{
    arrow::acero::Declaration source{"table_source", arrow::acero::TableSourceNodeOptions(table)};
    arrow::acero::Declaration aggregate{"aggregate", {source}, agg_opts};
    return arrow::acero::DeclarationToTable(std::move(aggregate)).ValueOrDie();
}

/*CkReductionMsg* aggregate_reducer(int nmsgs, CkReductionMsg** msgs)
{
    std::vector<TablePtr> reduction_tables;
    
    for (int i = 0; i < nmsgs; i++)
    {
        AggregateReductionMsg* msg = (AggregateReductionMsg*) msgs[i]->getData();
        arrow::acero::AggregateNodeOptions agg_opts = extract_aggregate_options(msg->get_options());
        if (msg->table_size != 0)
            reduction_tables.push_back(deserialize(msg->get_table(), msg->table_size));
    }

    AggregateReductionMsg* msg = (AggregateReductionMsg*) msgs[0]->getData();
    arrow::acero::AggregateNodeOptions agg_opts = extract_aggregate_options(msg->get_options());

    if (reduction_tables.size() > 0)
    {
        TablePtr combined_table = arrow::ConcatenateTables(reduction_tables).ValueOrDie();

        TablePtr result = local_aggregation(combined_table, agg_opts);

        BufferPtr out;
        serialize(result, out);

        AggregateReductionMsg* next_msg = create_reduction_msg(
            msg->result_name, out->size(), (char*) out->data(), msg->options_size, msg->get_options()
        );

        return CkReductionMsg::buildNew(sizeof(AggregateReductionMsg) + out->size() + msg->options_size, next_msg);
    }
    else
    {
        AggregateReductionMsg* next_msg = create_reduction_msg(
            msg->result_name, 0, nullptr, msg->options_size, msg->get_options()
        );
        return CkReductionMsg::buildNew(sizeof(AggregateReductionMsg) + msg->options_size, next_msg);
    }
}

void register_aggregate_reducer()
{
    AggregateReductionType = CkReduction::addReducer(aggregate_reducer);
}*/

#include "reduction.def.h"
#endif