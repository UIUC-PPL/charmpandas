#ifndef UTILS_H
#define UTILS_H

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/api.h>
#include <arrow/table.h>
#include <arrow/scalar.h>
#include <arrow/compute/api.h>
#include "types.hpp"

enum class Operation : int
{
    Read = 0,
    Fetch = 1,
    SetColumn = 2,
    GroupBy = 3,
    Join = 4,
    Print = 5,
    Concat = 6,
    Filter = 7,
    Rescale = 8,
    Skip = 9
};

template<class T>
inline T extract(char* &msg, bool increment=true)
{
    T arg = *(reinterpret_cast<T*>(msg));
    if (increment)
        msg += sizeof(T);
    return arg;
}

inline Operation lookup_operation(int opcode)
{
    return static_cast<Operation>(opcode);
}

inline TablePtr sort_table(TablePtr table, std::string col_name)
{
    arrow::compute::SortOptions sort_options;
    sort_options.sort_keys = {
        arrow::compute::SortKey(col_name, arrow::compute::SortOrder::Ascending)
    };

    // Compute the sort indices
    ArrayPtr indices = arrow::compute::SortIndices(arrow::Datum(table), sort_options).ValueOrDie();


    // Get the sorted table
    return arrow::compute::Take(table, indices).ValueOrDie().table();
}

#endif