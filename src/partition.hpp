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

private:
    CProxy_Main main_proxy;

    std::unordered_map<int, int> gather_count;
    std::unordered_map<int, std::vector<GatherTableDataMsg*>> gather_buffer;

    std::vector<int> local_chares;
    std::unordered_set<int> local_chares_set;

    // for joins
    int num_local_chares;
    int join_count;
    int join_odf;
    int expected_rows;

    TablePtr local_t1, local_t2;
    TablePtr join_left_table, join_right_table;

    JoinOptions* join_opts;

    int EPOCH;

public:
    CProxy_Partition partition_proxy;
    int num_partitions;

    Aggregator_SDAG_CODE

    Aggregator(CProxy_Main main_proxy_);

    Aggregator(CkMigrateMessage* m);

    void pup(PUP::er &p);

    void init_done();

    void register_local_chare(int index);

    void gather_table(GatherTableDataMsg* msg);
    
    void clear_gather_buffer(int epoch);

    void fetch_callback(int epoch, BufferPtr &out);

    void operation_join(char* cmd);

    void execute_command(int epoch, int size, char* cmd);

    void start_join();

    void update_histogram(TablePtr table, std::vector<int> &hist);

    TablePtr map_keys(TablePtr &table, std::vector<arrow::FieldRef> &fields);

    void assign_keys(int num_elements, int* global_hist);

    void shuffle_data(std::vector<int> pe_map, std::vector<int> expected_loads);

    void receive_shuffle_data(JoinShuffleTableMsg* msg);

    void complete_operation();

    void complete_join();

    TablePtr local_join(TablePtr &t1, TablePtr &t2, arrow::acero::HashJoinNodeOptions &opts);

    void partition_table(TablePtr table, int result_name);
};


class Partition : public CBase_Partition
{
private:
    CProxy_Aggregator agg_proxy;
    int num_partitions;
    int EPOCH;
    int join_count;
    bool local_join_done;
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

    void operation_read(char* cmd);

    void operation_fetch(char* cmd);

    void operation_print(char* cmd);

    void operation_concat(char* cmd);

    void operation_groupby(char* cmd);

    void operation_set_column(char* cmd);

    void operation_filter(char* cmd);

    void aggregate_result(CkReductionMsg* msg);

    void execute_command(int epoch, int size, char* cmd);

    arrow::Datum extract_operand(char* &msg);

    arrow::Datum traverse_ast(char* &msg);

    void read_parquet(int table_name, std::string file_path);
};

#endif