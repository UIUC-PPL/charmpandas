#ifndef GPU_OPS_H
#define GPU_OPS_H

#ifdef USE_GPU

#include <memory>
#include <string>
#include <vector>
#include <variant>

#include <cuda_runtime.h>
#include <cudf/binaryop.hpp>
#include <cudf/unary.hpp>
#include <cudf/reduction.hpp>
#include <cudf/sorting.hpp>
#include <cudf/copying.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/join.hpp>
#include <cudf/groupby.hpp>
#include <cudf/filling.hpp>
#include <cudf/hashing.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/io/parquet.hpp>
#include <cudf/aggregation.hpp>
#include <cudf/detail/aggregation/aggregation.hpp>

#include "types.hpp"
#include "gpu_table.hpp"

// ============================================================================
// GpuDatum: Polymorphic operand for GPU expression evaluation
// Equivalent to arrow::Datum but for cudf column/scalar
// ============================================================================

struct GpuDatum
{
    enum Kind { COLUMN, SCALAR } kind;
    std::unique_ptr<cudf::column> col;
    std::unique_ptr<cudf::scalar> scalar;

    GpuDatum() : kind(COLUMN) {}

    static GpuDatum from_column(std::unique_ptr<cudf::column> c)
    {
        GpuDatum d;
        d.kind = COLUMN;
        d.col = std::move(c);
        return d;
    }

    static GpuDatum from_scalar(std::unique_ptr<cudf::scalar> s)
    {
        GpuDatum d;
        d.kind = SCALAR;
        d.scalar = std::move(s);
        return d;
    }

    bool is_column() const { return kind == COLUMN; }
    bool is_scalar() const { return kind == SCALAR; }
};

// ============================================================================
// Binary operation mapping: ArrayOperation -> cudf::binary_operator
// ============================================================================

inline cudf::binary_operator to_cudf_binop(ArrayOperation op)
{
    switch (op)
    {
        case ArrayOperation::Add:          return cudf::binary_operator::ADD;
        case ArrayOperation::Sub:          return cudf::binary_operator::SUB;
        case ArrayOperation::Multiply:     return cudf::binary_operator::MUL;
        case ArrayOperation::Divide:       return cudf::binary_operator::DIV;
        case ArrayOperation::LessThan:     return cudf::binary_operator::LESS;
        case ArrayOperation::LessEqual:    return cudf::binary_operator::LESS_EQUAL;
        case ArrayOperation::GreaterThan:  return cudf::binary_operator::GREATER;
        case ArrayOperation::GreaterEqual: return cudf::binary_operator::GREATER_EQUAL;
        case ArrayOperation::Equal:        return cudf::binary_operator::EQUAL;
        case ArrayOperation::NotEqual:     return cudf::binary_operator::NOT_EQUAL;
        default:
            throw std::runtime_error("Unknown binary operation");
    }
}

// Determine output type for binary operations
inline cudf::data_type binop_output_type(ArrayOperation op,
    cudf::data_type lhs_type, cudf::data_type rhs_type)
{
    switch (op)
    {
        // Comparison ops always return bool
        case ArrayOperation::LessThan:
        case ArrayOperation::LessEqual:
        case ArrayOperation::GreaterThan:
        case ArrayOperation::GreaterEqual:
        case ArrayOperation::Equal:
        case ArrayOperation::NotEqual:
            return cudf::data_type{cudf::type_id::BOOL8};

        // Arithmetic ops: use the wider type
        default:
        {
            // If either is double, result is double
            if (lhs_type.id() == cudf::type_id::FLOAT64 ||
                rhs_type.id() == cudf::type_id::FLOAT64)
                return cudf::data_type{cudf::type_id::FLOAT64};
            // If either is float, result is float
            if (lhs_type.id() == cudf::type_id::FLOAT32 ||
                rhs_type.id() == cudf::type_id::FLOAT32)
                return cudf::data_type{cudf::type_id::FLOAT32};
            // Int64 or timestamp
            if (lhs_type.id() == cudf::type_id::INT64 ||
                rhs_type.id() == cudf::type_id::INT64 ||
                lhs_type.id() == cudf::type_id::TIMESTAMP_NANOSECONDS ||
                rhs_type.id() == cudf::type_id::TIMESTAMP_NANOSECONDS)
                return cudf::data_type{cudf::type_id::INT64};
            return cudf::data_type{cudf::type_id::INT32};
        }
    }
}

// ============================================================================
// GPU binary operation execution (column-column, column-scalar, scalar-column)
// ============================================================================

inline std::unique_ptr<cudf::column> execute_operation_gpu(
    GpuDatum& lhs, GpuDatum& rhs, ArrayOperation op)
{
    auto binop = to_cudf_binop(op);

    if (lhs.is_column() && rhs.is_column())
    {
        auto out_type = binop_output_type(op,
            lhs.col->view().type(), rhs.col->view().type());
        return cudf::binary_operation(
            lhs.col->view(), rhs.col->view(), binop, out_type);
    }
    else if (lhs.is_column() && rhs.is_scalar())
    {
        auto out_type = binop_output_type(op,
            lhs.col->view().type(), rhs.scalar->type());
        return cudf::binary_operation(
            lhs.col->view(), *rhs.scalar, binop, out_type);
    }
    else if (lhs.is_scalar() && rhs.is_column())
    {
        auto out_type = binop_output_type(op,
            lhs.scalar->type(), rhs.col->view().type());
        return cudf::binary_operation(
            *lhs.scalar, rhs.col->view(), binop, out_type);
    }
    else
    {
        // scalar-scalar: should not happen in practice
        throw std::runtime_error("GPU binary op: scalar-scalar not supported");
    }
}

// ============================================================================
// Parquet I/O — direct to GPU
// ============================================================================

inline TablePtr gpu_read_parquet(const std::string& file_path,
    int64_t skip_rows, int64_t num_rows)
{
    auto source = cudf::io::source_info{file_path};
    auto opts_builder = cudf::io::parquet_reader_options::builder(source);

    if (skip_rows > 0)
        opts_builder.skip_rows(skip_rows);
    if (num_rows > 0)
        opts_builder.num_rows(num_rows);

    auto opts = opts_builder.build();
    auto result = cudf::io::read_parquet(opts);

    // Extract column names from metadata
    std::vector<std::string> names;
    names.reserve(result.metadata.schema_info.size());
    for (auto const& col_info : result.metadata.schema_info)
        names.push_back(col_info.name);

    return std::make_shared<GpuTable>(std::move(result.tbl), std::move(names));
}

// Read parquet and get total row count (for partitioning logic)
inline int64_t gpu_parquet_num_rows(const std::string& file_path)
{
    auto source = cudf::io::source_info{file_path};
    auto opts = cudf::io::parquet_reader_options::builder(source)
        .num_rows(0)
        .build();
    auto result = cudf::io::read_parquet(opts);
    return result.metadata.num_rows_per_source[0];
}

// ============================================================================
// Filter — boolean mask
// ============================================================================

inline TablePtr gpu_filter(TablePtr& table, cudf::column_view bool_mask)
{
    auto result = cudf::apply_boolean_mask(table->view(), bool_mask);
    return std::make_shared<GpuTable>(std::move(result),
        std::vector<std::string>(table->field_names()));
}

// ============================================================================
// Concatenate tables
// ============================================================================

inline TablePtr gpu_concatenate(const std::vector<TablePtr>& tables)
{
    std::vector<cudf::table_view> views;
    views.reserve(tables.size());
    for (auto const& t : tables)
        views.push_back(t->view());

    auto result = cudf::concatenate(views);
    return std::make_shared<GpuTable>(std::move(result),
        std::vector<std::string>(tables[0]->field_names()));
}

// ============================================================================
// Sort
// ============================================================================

inline TablePtr gpu_sort(TablePtr& table,
    const std::vector<cudf::size_type>& key_indices,
    const std::vector<cudf::order>& orders)
{
    // Build key column views for sort
    std::vector<cudf::column_view> key_views;
    for (auto idx : key_indices)
        key_views.push_back(table->view().column(idx));
    cudf::table_view keys_table(key_views);

    auto sorted_order = cudf::sorted_order(keys_table, orders);
    auto sorted = cudf::gather(table->view(), sorted_order->view());
    return std::make_shared<GpuTable>(std::move(sorted),
        std::vector<std::string>(table->field_names()));
}

// Overload: sort by column names
inline TablePtr gpu_sort(TablePtr& table,
    const std::vector<std::string>& key_names,
    const std::vector<cudf::order>& orders)
{
    std::vector<cudf::size_type> key_indices;
    for (auto const& name : key_names)
        key_indices.push_back(table->GetColumnIndex(name));
    return gpu_sort(table, key_indices, orders);
}

// ============================================================================
// Reduction
// ============================================================================

inline std::unique_ptr<cudf::aggregation> make_gpu_aggregation(AggregateOperation op)
{
    switch (op)
    {
        case AggregateOperation::Sum:
            return cudf::make_sum_aggregation<cudf::reduce_aggregation>();
        case AggregateOperation::Count:
            return cudf::make_count_aggregation<cudf::reduce_aggregation>();
        case AggregateOperation::All:
            return cudf::make_all_aggregation<cudf::reduce_aggregation>();
        case AggregateOperation::Any:
            return cudf::make_any_aggregation<cudf::reduce_aggregation>();
        case AggregateOperation::ApproximateMedian:
            return cudf::make_median_aggregation<cudf::reduce_aggregation>();
        case AggregateOperation::First:
            return cudf::make_nth_element_aggregation<cudf::reduce_aggregation>(0);
        case AggregateOperation::Last:
            return cudf::make_nth_element_aggregation<cudf::reduce_aggregation>(-1);
        default:
            throw std::runtime_error("Unsupported GPU reduction aggregation");
    }
}

inline std::unique_ptr<cudf::scalar> gpu_reduce(
    cudf::column_view col, AggregateOperation op)
{
    auto agg = make_gpu_aggregation(op);
    return cudf::reduce(col,
        *dynamic_cast<cudf::reduce_aggregation*>(agg.get()),
        col.type());
}

// ============================================================================
// Hashing (replaces xxhash row-by-row with GPU column hashing)
// ============================================================================

inline std::unique_ptr<cudf::column> gpu_hash_keys(
    cudf::table_view key_columns)
{
    return cudf::hashing::murmurhash3_x86_32(key_columns);
}

// ============================================================================
// Join
// ============================================================================

inline TablePtr gpu_join(TablePtr& left, TablePtr& right,
    const std::vector<cudf::size_type>& left_on,
    const std::vector<cudf::size_type>& right_on,
    arrow::acero::JoinType join_type)
{
    std::unique_ptr<rmm::device_uvector<cudf::size_type>> left_indices;
    std::unique_ptr<rmm::device_uvector<cudf::size_type>> right_indices;

    // Build key table views
    std::vector<cudf::column_view> left_key_views, right_key_views;
    for (auto idx : left_on)
        left_key_views.push_back(left->view().column(idx));
    for (auto idx : right_on)
        right_key_views.push_back(right->view().column(idx));

    cudf::table_view left_keys(left_key_views);
    cudf::table_view right_keys(right_key_views);

    switch (join_type)
    {
        case arrow::acero::JoinType::INNER:
        {
            auto [li, ri] = cudf::inner_join(left_keys, right_keys);
            left_indices = std::move(li);
            right_indices = std::move(ri);
            break;
        }
        case arrow::acero::JoinType::LEFT_OUTER:
        {
            auto [li, ri] = cudf::left_join(left_keys, right_keys);
            left_indices = std::move(li);
            right_indices = std::move(ri);
            break;
        }
        case arrow::acero::JoinType::RIGHT_OUTER:
        {
            // Right join = left join with swapped tables
            auto [ri, li] = cudf::left_join(right_keys, left_keys);
            left_indices = std::move(li);
            right_indices = std::move(ri);
            break;
        }
        case arrow::acero::JoinType::FULL_OUTER:
        {
            auto [li, ri] = cudf::full_join(left_keys, right_keys);
            left_indices = std::move(li);
            right_indices = std::move(ri);
            break;
        }
        case arrow::acero::JoinType::LEFT_SEMI:
        {
            auto li = cudf::left_semi_join(left_keys, right_keys);
            auto gathered = cudf::gather(left->view(),
                cudf::column_view(cudf::data_type{cudf::type_id::INT32},
                    li->size(), li->data()));
            return std::make_shared<GpuTable>(std::move(gathered),
                std::vector<std::string>(left->field_names()));
        }
        case arrow::acero::JoinType::LEFT_ANTI:
        {
            auto li = cudf::left_anti_join(left_keys, right_keys);
            auto gathered = cudf::gather(left->view(),
                cudf::column_view(cudf::data_type{cudf::type_id::INT32},
                    li->size(), li->data()));
            return std::make_shared<GpuTable>(std::move(gathered),
                std::vector<std::string>(left->field_names()));
        }
        default:
            throw std::runtime_error("Unsupported join type");
    }

    // Gather rows from both tables using the join indices
    cudf::column_view left_indices_col(cudf::data_type{cudf::type_id::INT32},
        left_indices->size(), left_indices->data());
    cudf::column_view right_indices_col(cudf::data_type{cudf::type_id::INT32},
        right_indices->size(), right_indices->data());

    auto left_gathered = cudf::gather(left->view(), left_indices_col);
    auto right_gathered = cudf::gather(right->view(), right_indices_col);

    // Combine columns: left columns + non-key right columns
    std::vector<std::unique_ptr<cudf::column>> result_cols;
    std::vector<std::string> result_names;

    // All left columns
    auto left_cols = left_gathered->release();
    for (size_t i = 0; i < left_cols.size(); i++)
    {
        result_cols.push_back(std::move(left_cols[i]));
        result_names.push_back(left->field_names()[i]);
    }

    // Non-key right columns
    std::set<cudf::size_type> right_key_set(right_on.begin(), right_on.end());
    auto right_cols = right_gathered->release();
    for (size_t i = 0; i < right_cols.size(); i++)
    {
        if (right_key_set.find(i) == right_key_set.end())
        {
            result_cols.push_back(std::move(right_cols[i]));
            result_names.push_back(right->field_names()[i]);
        }
    }

    auto result_table = std::make_unique<cudf::table>(std::move(result_cols));
    return std::make_shared<GpuTable>(std::move(result_table), std::move(result_names));
}

// ============================================================================
// GroupBy
// ============================================================================

inline std::unique_ptr<cudf::groupby_aggregation> make_gpu_groupby_aggregation(
    AggregateOperation op)
{
    switch (op)
    {
        case AggregateOperation::Sum:
            return cudf::make_sum_aggregation<cudf::groupby_aggregation>();
        case AggregateOperation::Count:
            return cudf::make_count_aggregation<cudf::groupby_aggregation>();
        case AggregateOperation::All:
            return cudf::make_all_aggregation<cudf::groupby_aggregation>();
        case AggregateOperation::Any:
            return cudf::make_any_aggregation<cudf::groupby_aggregation>();
        case AggregateOperation::ApproximateMedian:
            return cudf::make_median_aggregation<cudf::groupby_aggregation>();
        case AggregateOperation::CountDistinct:
            return cudf::make_nunique_aggregation<cudf::groupby_aggregation>();
        case AggregateOperation::First:
            return cudf::make_nth_element_aggregation<cudf::groupby_aggregation>(0);
        case AggregateOperation::Last:
            return cudf::make_nth_element_aggregation<cudf::groupby_aggregation>(-1);
        default:
            throw std::runtime_error("Unsupported GPU groupby aggregation");
    }
}

// ============================================================================
// GroupBy
// ============================================================================

inline TablePtr gpu_groupby(TablePtr& table,
    const std::vector<std::string>& key_names,
    const std::vector<std::pair<AggregateOperation, std::string>>& aggregations,
    const std::vector<std::string>& result_names)
{
    // Build key column views
    std::vector<cudf::column_view> key_views;
    for (auto const& name : key_names)
        key_views.push_back(table->GetColumnByName(name));

    cudf::table_view keys_table(key_views);
    cudf::groupby::groupby gb(keys_table);

    // Build aggregation requests
    std::vector<cudf::groupby::aggregation_request> requests;
    for (auto const& [op, target_col] : aggregations)
    {
        cudf::groupby::aggregation_request req;
        req.values = table->GetColumnByName(target_col);
        req.aggregations.push_back(make_gpu_groupby_aggregation(op));
        requests.push_back(std::move(req));
    }

    auto [keys_result, agg_results] = gb.aggregate(requests);

    // Build result table: key columns + aggregation result columns
    std::vector<std::unique_ptr<cudf::column>> result_cols;
    std::vector<std::string> result_col_names;

    // Key columns
    auto key_cols = keys_result->release();
    for (size_t i = 0; i < key_cols.size(); i++)
    {
        result_cols.push_back(std::move(key_cols[i]));
        result_col_names.push_back(key_names[i]);
    }

    // Aggregation result columns
    for (size_t i = 0; i < agg_results.size(); i++)
    {
        auto& result = agg_results[i].results[0];
        result_cols.push_back(std::move(result));
        result_col_names.push_back(result_names[i]);
    }

    auto result_table = std::make_unique<cudf::table>(std::move(result_cols));
    return std::make_shared<GpuTable>(std::move(result_table), std::move(result_col_names));
}

// ============================================================================
// Add metadata columns (local_index, home_partition)
// ============================================================================

inline TablePtr gpu_add_metadata(TablePtr& table, int partition_index)
{
    cudf::size_type nrows = table->num_rows();

    // local_index: 0, 1, 2, ..., nrows-1
    auto init_scalar = cudf::make_fixed_width_scalar<int32_t>(0);
    auto step_scalar = cudf::make_fixed_width_scalar<int32_t>(1);
    auto local_index = cudf::sequence(nrows, *init_scalar, *step_scalar);

    // home_partition: constant column filled with partition_index
    auto home_scalar = cudf::make_fixed_width_scalar<int32_t>(partition_index);
    home_scalar->set_valid_async(true);
    auto home_partition = cudf::make_column_from_scalar(*home_scalar, nrows);

    // Add both columns
    auto result = table->SetColumn("local_index", std::move(local_index));
    result = result->SetColumn("home_partition", std::move(home_partition));
    return result;
}

// ============================================================================
// Clean metadata columns (remove local_index, home_partition, _mapped_key)
// ============================================================================

inline TablePtr gpu_clean_metadata(TablePtr& table)
{
    std::vector<int> keep_indices;
    auto const& names = table->field_names();
    for (int i = 0; i < static_cast<int>(names.size()); i++)
    {
        if (names[i].rfind("local_index", 0) == 0 ||
            names[i].rfind("home_partition", 0) == 0 ||
            names[i].rfind("_mapped_key", 0) == 0)
            continue;
        keep_indices.push_back(i);
    }
    return table->SelectColumns(keep_indices);
}

#endif // USE_GPU
#endif // GPU_OPS_H
