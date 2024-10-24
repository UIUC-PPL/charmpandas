#ifndef OPERATIONS_H
#define OPERATIONS_H

#include <string>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/result.h>
#include "utils.hpp"

enum class ArrayOperation : int
{
    Noop = 0,
    Add = 1,
    Sub = 2,
    Multiply = 3,
    Divide = 4,
    LessThan = 5,
    LessEqual = 6,
    GreaterThan = 7,
    GreaterEqual = 8,
    Equal = 9,
    NotEqual = 10
};

enum class OperandType : int
{
    Field = 0,
    Integer = 1,
    Double = 2
};

std::string get_array_operation(ArrayOperation op)
{
    switch (op)
    {
        case ArrayOperation::Add:
            return "add";

        case ArrayOperation::Sub:
            return "subtract";

        case ArrayOperation::Multiply:
            return "multiply";

        case ArrayOperation::Divide:
            return "divide";

        case ArrayOperation::LessThan:
            return "less";

        case ArrayOperation::LessEqual:
            return "less_equal";

        case ArrayOperation::GreaterThan:
            return "greater";

        case ArrayOperation::GreaterEqual:
            return "greater_equal";

        case ArrayOperation::Equal:
            return "equal";

        case ArrayOperation::NotEqual:
            return "not_equal";
        
        default:
            return "noop";
    }
}

inline arrow::Datum execute_operation(ArrayOperation op, std::vector<arrow::Datum> operands)
{
    return arrow::compute::CallFunction(get_array_operation(op), operands).ValueOrDie();
}

inline TablePtr set_column(TablePtr table, std::string &field_name, arrow::Datum &result_datum)
{
    ChunkedArrayPtr result = result_datum.chunked_array();
    auto result_field = arrow::field(field_name, result->type());
    int field_index = table->schema()->GetFieldIndex(field_name);
    if (field_index == -1)
        return table->AddColumn(table->num_columns(), result_field, result).ValueOrDie();
    else
        return table->SetColumn(field_index, result_field, result).ValueOrDie();
}

#endif