#ifndef TYPES_H
#define TYPES_H

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/api.h>
#include <arrow/table.h>
#include <arrow/scalar.h>
#include <arrow/compute/api.h>

using TablePtr = std::shared_ptr<arrow::Table>;
using ArrayPtr = std::shared_ptr<arrow::Array>;
using ScalarPtr = std::shared_ptr<arrow::Scalar>;
using ChunkedArrayPtr = std::shared_ptr<arrow::ChunkedArray>;
using BufferPtr = std::shared_ptr<arrow::Buffer>;

#endif