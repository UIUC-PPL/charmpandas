#ifndef PARTITION_H
#define PARTITION_H

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
#include "types.hpp"
#include "partition.decl.h"


#define RIGHT 0
#define LEFT 1


// This needs to have client id in the key
extern std::unordered_map<int, CcsDelayedReply> fetch_reply;

TablePtr clean_metadata(TablePtr &table);

class JoinOptions
{
public:
    int table1, table2;
    int result_name;
    arrow::acero::HashJoinNodeOptions* opts;

    JoinOptions(int table1_, int table2_, int result_name_, arrow::acero::HashJoinNodeOptions* opts_)
        : table1(table1_)
        , table2(table2_)
        , result_name(result_name_)
        , opts(opts_)
    {}
};

class GroupByOptions
{
public:
    int table_name;
    int result_name;
    arrow::acero::AggregateNodeOptions* opts;

    GroupByOptions(int table_name_, int result_name_, arrow::acero::AggregateNodeOptions* opts_)
        : table_name(table_name_)
        , result_name(result_name_)
        , opts(opts_)
    {}
};

class PELoad
{
public:
    int pe, load;

    PELoad(int pe_, int load_)
        : pe(pe_)
        , load(load_)
    {}

    bool operator>(const PELoad& other) const
    {
        return load > other.load;
    }
};

class Main : public CBase_Main
{
public:
    CProxy_Partition partition;

    Main(CkArgMsg* msg);

    Main(CkMigrateMessage *m);

    void pup(PUP::er &p);

    void register_handlers();

    void init_done();
};

// This should not be a group!
class Aggregator : public CBase_Aggregator
{
    using RemoteBuffer = std::unordered_map<TablePtr, std::pair<bool, bool>>;
    using RemoteMsgBuffer = std::unordered_map<TablePtr, std::pair<RemoteJoinMsg*, TablePtr>>;
    using CallbackType = void (Aggregator::*)(void);

private:
    CProxy_Main main_proxy;

    std::unordered_map<int, int> gather_count;
    std::unordered_map<int, std::vector<GatherTableDataMsg*>> gather_buffer;

    std::vector<int> local_chares;

    // for joins
    int num_local_chares;
    int redist_odf;
    int expected_rows;
    int next_temp_name;

    std::vector<int> redist_table_names;
    std::unordered_map<int, TablePtr> tables;
    std::unordered_map<int, TablePtr> redist_tables;

    RedistOperation redist_operation;

    JoinOptions* join_opts;
    GroupByOptions* groupby_opts;

    int EPOCH;

public:
    CProxy_Partition partition_proxy;
    int num_partitions;

    Aggregator_SDAG_CODE

    Aggregator(CProxy_Main main_proxy_);

    Aggregator(CkMigrateMessage* m);

    void pup(PUP::er &p);

    void init_memory_logging();

    static void log_memory_usage(void* msg, double curr_time);

    void init_done();

    TablePtr get_local_table(int table_name);

    void clear_local_chares();

    void register_local_chare(int index);

    void gather_table(GatherTableDataMsg* msg);
    
    void clear_gather_buffer(int epoch);

    void fetch_callback(int epoch, BufferPtr &out);

    void deposit_size(int partition, int local_size);

    void handle_deletions(char* &cmd);

    void redistribute(std::vector<int> table_names, std::vector<std::vector<arrow::FieldRef>> &keys,
        RedistOperation oper);

    void redist_callback();

    void groupby_callback();

    void join_callback();

    void operation_join(char* cmd);

    void operation_groupby(char* cmd);

    //void operation_barrier(char* cmd);

    void execute_command(int epoch, int size, char* cmd);

    void start_join();

    void update_histogram(TablePtr table, std::vector<int> &hist);

    TablePtr map_keys(TablePtr &table, std::vector<arrow::FieldRef> &fields);

    void assign_keys(int num_elements, int* global_hist);

    void shuffle_data(std::vector<int> pe_map, std::vector<int> expected_loads);

    void receive_shuffle_data(RedistTableMsg* msg);

    void complete_operation();

    void complete_groupby();

    void complete_join();

    TablePtr local_join(TablePtr &t1, TablePtr &t2, arrow::acero::HashJoinNodeOptions &opts);

    void partition_table(TablePtr table, int result_name);

    void barrier_handler(int epoch);

    //void aggregator_barrier(int agg_epoch);

    //template<typename T>
    void reduction_result_int(int result, int epoch);

    void reduction_result_long(int64_t result, int epoch);

    void reduction_result_float(float result, int epoch);

    void start_polling();
};


class Partition : public CBase_Partition
{
private:
    CProxy_Aggregator agg_proxy;
    int num_partitions;
    int EPOCH;
    int lb_period;
    std::unordered_map<int, TablePtr> tables;
    std::unordered_map<int, BufferPtr> tables_serialized;

public:
    Partition_SDAG_CODE

    Partition(int num_partitions_, int lb_period_, CProxy_Aggregator agg_proxy_);

    Partition(CkMigrateMessage* m);

    ~Partition();

    void pup(PUP::er &p);

    void pup_tables(PUP::er &p);

    int64_t calculate_memory_usage();

    int64_t calculate_memory_usage(TablePtr table);

    void serialize_and_cache(int table_name);

    void complete_operation();

    inline void inc_epoch();

    void ResumeFromSync();

    TablePtr get_table(int table_name);

    TablePtr get_table(int table_name, std::vector<std::string> fields);

    inline void add_table(int table_name, TablePtr table);

    inline void remove_table(int table_name);

    void handle_deletions(char* &cmd);

    void operation_read(char* cmd);

    void operation_fetch(char* cmd);

    void operation_print(char* cmd);

    void operation_concat(char* cmd);

    void operation_groupby(char* cmd);

    void operation_set_column(char* cmd);

    void operation_filter(char* cmd);

    void operation_fetch_size(char* cmd);

    void operation_barrier(char* cmd);

    void operation_reduction(char* cmd);

    ScalarPtr local_reduction(TablePtr& table, std::string& col_name, AggregateOperation& op);

    void aggregate_result(CkReductionMsg* msg);

    void execute_command(int epoch, int size, char* cmd);

    arrow::Datum extract_operand(char* &msg);

    arrow::Datum traverse_ast(char* &msg);

    void read_parquet(int table_name, std::string file_path);

    template<typename T>
    void reduce_scalar(ScalarPtr& scalar, AggregateOperation& op);

    void reduction_result_int(int result);

    void reduction_result_long(int64_t result);

    void reduction_result_float(float result);
};

#endif