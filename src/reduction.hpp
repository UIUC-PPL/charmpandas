#ifndef REDUCTION_H
#define REDUCTION_H

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/api.h>
#include <arrow/table.h>
#include <arrow/compute/api.h>
#include "arrow/acero/exec_plan.h"
#include <arrow/acero/options.h>
#include "utils.hpp"
#include "serialize.hpp"
#include "reduction.decl.h"

CkReduction::reducerType AggregateReductionType;

class AggregateReductionMsg : public CMessage_AggregateReductionMsg
{
public:
    char* table;
    char* options;
    int result_name;
    int table_size;
    int options_size;

    AggregateReductionMsg(int result_name_, int table_size_, int options_size_)
        : result_name(result_name_)
        , table_size(table_size_)
        , options_size(options_size_)
    {}
};

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
    }
}

arrow::acero::AggregateNodeOptions extract_aggregate_options(char* msg, bool is_local=false)
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
        CkPrintf("agg_fn = %s, target = %s, result = %s\n", agg_fn.c_str(), target_field.c_str(), result_field.c_str());
        aggs.push_back({agg_fn, nullptr, is_local ? target_field : result_field, result_field});
    }

    return arrow::acero::AggregateNodeOptions{aggs, keys};
}

TablePtr local_aggregation(TablePtr &table, arrow::acero::AggregateNodeOptions &agg_opts)
{
    arrow::acero::Declaration source{"table_source", arrow::acero::TableSourceNodeOptions(table)};
    arrow::acero::Declaration aggregate{"aggregate", {source}, agg_opts};
    return arrow::acero::DeclarationToTable(std::move(aggregate)).ValueOrDie();
}

CkReductionMsg* aggregate_reducer(int nmsgs, CkReductionMsg** msgs)
{
    std::vector<TablePtr> reduction_tables;

    CkPrintf("PE = %i, nmsgs = %i\n", CkMyPe(), nmsgs);
    
    for (int i = 0; i < nmsgs; i++)
    {
        AggregateReductionMsg* msg = (AggregateReductionMsg*) msgs[i]->getData();
        CkPrintf("Extracting options on PE %i msg %i\n", CkMyPe(), i);
        arrow::acero::AggregateNodeOptions agg_opts = extract_aggregate_options(msg->options);
        CkPrintf("Calling deserialize on PE %i, msg %i\n", CkMyPe(), i);
        if (msg->table != nullptr)
            reduction_tables.push_back(deserialize(msg->table, msg->table_size));
        CkPrintf("Done deserialization on PE %i, msg %i\n", CkMyPe(), i);
    }

    AggregateReductionMsg* msg = (AggregateReductionMsg*) msgs[0]->getData();
    CkPrintf("Options ptr before = %p\n", msg->options);
    arrow::acero::AggregateNodeOptions agg_opts = extract_aggregate_options(msg->options);
    CkPrintf("Options ptr after = %p\n", msg->options);

    if (reduction_tables.size() > 0)
    {
        //CkPrintf("Deserialization done on PE %i\n", CkMyPe());
        TablePtr combined_table = arrow::ConcatenateTables(reduction_tables).ValueOrDie();

        TablePtr result = local_aggregation(combined_table, agg_opts);

        //CkPrintf("Groupby result %i ->\n%s\n%s\n", CkMyPe(), combined_table->schema()->ToString().c_str(), 
        //    result->schema()->ToString().c_str());
        
        BufferPtr out;
        serialize(result, out);

        AggregateReductionMsg* next_msg = new (out->size(), msg->options_size) AggregateReductionMsg(
            msg->result_name, out->size(), msg->options_size
        );
        std::memcpy(next_msg->table, out->data(), out->size());
        std::memcpy(next_msg->options, msg->options, msg->options_size);

        //CkPrintf("TEST\n");
        //arrow::acero::AggregateNodeOptions agg_opts = extract_aggregate_options(next_msg->options);

        return CkReductionMsg::buildNew(3*sizeof(int) + out->size() + msg->options_size, next_msg);
    }
    else
    {
        AggregateReductionMsg* next_msg = new (0, msg->options_size) AggregateReductionMsg(
            msg->result_name, 0, msg->options_size
        );
        next_msg->table = nullptr;
        std::memcpy(next_msg->options, msg->options, msg->options_size);
        return CkReductionMsg::buildNew(3*sizeof(int) + msg->options_size, next_msg);
    }
}

void register_aggregate_reducer()
{
    AggregateReductionType = CkReduction::addReducer(aggregate_reducer);
}

#include "reduction.def.h"
#endif