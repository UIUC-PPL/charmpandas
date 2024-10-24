#ifndef UTILS_H
#define UTILS_H

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/api.h>
#include <arrow/table.h>
#include <arrow/scalar.h>

using TablePtr = std::shared_ptr<arrow::Table>;
using ArrayPtr = std::shared_ptr<arrow::Array>;
using ScalarPtr = std::shared_ptr<arrow::Scalar>;
using ChunkedArrayPtr = std::shared_ptr<arrow::ChunkedArray>;
using BufferPtr = std::shared_ptr<arrow::Buffer>;

enum class Operation : int
{
    Read = 0,
    Fetch = 1,
    SetColumn = 2,
    GroupBy = 3,
    Join = 4,
    Print = 5,
    Concat = 6
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

#endif