#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/api.h>
#include <arrow/table.h>
#include <arrow/compute/api.h>
#include "arrow/acero/exec_plan.h"
#include <arrow/acero/options.h>
#include "utils.hpp"
#include "partition.decl.h"


class GroupByReductionMsg : public CMessage_GroupByReductionMsg
{
public:
    char* table;
    char* options;
    int table_size;
    int options_size;

    GroupByReductionMsg(int table_size_, int options_size_)
        : table_size(table_size_)
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

void extract_aggregate_options(char* msg)
{
    // first extract the keys
    int nkeys = extract<int>(msg);
    std::vector<std::string> keys;
    for (int i = 0; i < nkeys; i++)
    {
        int key_size = extract<int>(msg);
        std::string key(msg, key_size);
        msg += key_size;
        keys.push_back(key);
    }

    // then extract the aggregations
    std::vector<arrow::compute::Aggregate> aggs;
    int naggs = extract<int>(cmd);
    for (int i = 0; i < naggs; i++)
    {
        AggregateOperation agg_op = static_cast<AggregateOperation>(extract<int>(cmd));
        std::string agg_fn = get_aggregation_function(agg_op);
        int target_size = extract<int>(msg);
        std::string target_field(msg, target_size);
        msg += target_size;
        int result_size = extract<int>(msg);
        std::string result_field(msg, result_size);
        msg += result_size;
        aggs.push_back({agg_fn, nullptr, target_field, result_field});
    }

    return arrow::acero::AggregateNodeOptions{aggs, keys};
}

TablePtr local_groupby(TablePtr &table, arrow::acero::AggregateNodeOptions &agg_opts)
{
    arrow::acero::Declaration source{"table_source", arrow::acero::TableSourceNodeOptions(table)};
    arrow::acero::Declaration aggregate{"aggregate", {source}, agg_opts};
    return arrow::acero::DeclarationToTable(std::move(aggregate)).ValueOrDie();
}

CkReductionMsg* groupby_reducer(int nmsgs, CkReductionMsg** msgs)
{
    std::vector<TablePtr> reduction_tables;
    
    arrow::acero::AggregateNodeOptions agg_opts = extract_aggregate_options(msg->options);

    for (int i = 0; i < nmsgs; i++)
    {
        GroupByReductionMsg* msg = (GroupByReductionMsg*) msgs[i]->getData();
        reduction_tables.push_back(deserialize(msg->table, msg->table_size));
    }

    TablePtr combined_table = arrow::ConcatenateTables(reduction_tables).ValueOrDie();
    
    TablePtr result = local_groupby(combined_table, agg_opts);
    
    BufferPtr out;
    serialize(result, out);
}

#include "partition.def.h"