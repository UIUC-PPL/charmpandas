#ifndef GPU_TABLE_H
#define GPU_TABLE_H

#ifdef USE_GPU

#include <memory>
#include <string>
#include <vector>
#include <stdexcept>
#include <algorithm>

#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/copying.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/interop.hpp>
#include <cudf/contiguous_split.hpp>

#include <rmm/device_buffer.hpp>

#include <arrow/api.h>
#include <arrow/table.h>

using ArrowTablePtr = std::shared_ptr<arrow::Table>;

class GpuTable
{
private:
    std::unique_ptr<cudf::table> table_;
    std::vector<std::string> column_names_;

public:
    GpuTable() = default;

    GpuTable(std::unique_ptr<cudf::table> table, std::vector<std::string> column_names)
        : table_(std::move(table))
        , column_names_(std::move(column_names))
    {}

    // Move-only (cudf::table is move-only)
    GpuTable(GpuTable&&) = default;
    GpuTable& operator=(GpuTable&&) = default;
    GpuTable(const GpuTable&) = delete;
    GpuTable& operator=(const GpuTable&) = delete;

    // --- Accessors ---

    cudf::table_view view() const { return table_->view(); }

    cudf::size_type num_rows() const { return table_->num_rows(); }

    cudf::size_type num_columns() const { return table_->num_columns(); }

    std::vector<std::string> const& field_names() const { return column_names_; }

    cudf::table& mutable_table() { return *table_; }

    std::unique_ptr<cudf::table>& release_table() { return table_; }

    int GetColumnIndex(const std::string& name) const
    {
        auto it = std::find(column_names_.begin(), column_names_.end(), name);
        if (it == column_names_.end())
            return -1;
        return static_cast<int>(std::distance(column_names_.begin(), it));
    }

    cudf::column_view GetColumnByName(const std::string& name) const
    {
        int idx = GetColumnIndex(name);
        if (idx < 0)
            throw std::runtime_error("Column not found: " + name);
        return table_->view().column(idx);
    }

    // --- Arrow interop (for Python fetch boundary only) ---

    ArrowTablePtr to_arrow() const
    {
        // Build column metadata for cudf::to_arrow
        cudf::column_metadata root;
        root.children_meta.reserve(column_names_.size());
        for (auto const& name : column_names_)
        {
            cudf::column_metadata col_meta;
            col_meta.name = name;
            root.children_meta.push_back(col_meta);
        }

        std::vector<cudf::column_metadata> metadata;
        metadata.reserve(column_names_.size());
        for (auto const& name : column_names_)
        {
            cudf::column_metadata col_meta;
            col_meta.name = name;
            metadata.push_back(col_meta);
        }

        return cudf::to_arrow(table_->view(), metadata);
    }

    static std::shared_ptr<GpuTable> from_arrow(ArrowTablePtr arrow_table)
    {
        // Extract column names from Arrow schema
        std::vector<std::string> names;
        names.reserve(arrow_table->num_columns());
        for (auto const& field : arrow_table->schema()->fields())
            names.push_back(field->name());

        auto cudf_table = cudf::from_arrow(*arrow_table);
        return std::make_shared<GpuTable>(std::move(cudf_table), std::move(names));
    }

    // --- GPU-native serialization (for Charm++ RDMA) ---

    cudf::packed_columns pack() const
    {
        return cudf::pack(table_->view());
    }

    static std::shared_ptr<GpuTable> unpack(
        const uint8_t* metadata_ptr, size_t metadata_size,
        const uint8_t* gpu_data_ptr, size_t gpu_data_size,
        std::vector<std::string> names)
    {
        // Reconstruct packed_columns metadata
        auto metadata = std::make_unique<std::vector<uint8_t>>(
            metadata_ptr, metadata_ptr + metadata_size);

        // Wrap device pointer as device_buffer (non-owning view via device_span)
        // We need to copy into an owning buffer for unpack
        rmm::device_buffer device_buf(gpu_data_ptr, gpu_data_size,
            rmm::cuda_stream_default);

        cudf::packed_columns packed;
        packed.metadata = std::move(metadata);
        packed.gpu_data = std::move(device_buf);

        auto result = cudf::unpack(packed);
        // unpack returns a table with new owning columns
        return std::make_shared<GpuTable>(
            std::make_unique<cudf::table>(result), std::move(names));
    }

    // --- Table operations ---

    std::shared_ptr<GpuTable> SelectColumns(const std::vector<int>& indices) const
    {
        std::vector<cudf::size_type> cudf_indices(indices.begin(), indices.end());

        // Build selected column views
        std::vector<cudf::column_view> selected_views;
        selected_views.reserve(indices.size());
        for (int idx : indices)
            selected_views.push_back(table_->view().column(idx));

        cudf::table_view selected_view(selected_views);
        auto selected_table = std::make_unique<cudf::table>(selected_view);

        // Build new column names
        std::vector<std::string> new_names;
        new_names.reserve(indices.size());
        for (int idx : indices)
            new_names.push_back(column_names_[idx]);

        return std::make_shared<GpuTable>(std::move(selected_table), std::move(new_names));
    }

    std::shared_ptr<GpuTable> Slice(cudf::size_type offset, cudf::size_type length) const
    {
        auto sliced_views = cudf::slice(table_->view(), {offset, offset + length});
        // Copy the slice into an owning table
        auto sliced_table = std::make_unique<cudf::table>(sliced_views[0]);
        return std::make_shared<GpuTable>(std::move(sliced_table),
            std::vector<std::string>(column_names_));
    }

    // --- Column manipulation (cudf::table is immutable, must rebuild) ---

    // Add or replace a column by name
    std::shared_ptr<GpuTable> SetColumn(const std::string& name,
        std::unique_ptr<cudf::column> new_col) const
    {
        int existing_idx = GetColumnIndex(name);

        // Extract all columns from current table
        auto cols = table_->release();
        std::vector<std::string> new_names = column_names_;

        if (existing_idx >= 0)
        {
            // Replace existing column
            cols[existing_idx] = std::move(new_col);
        }
        else
        {
            // Append new column
            cols.push_back(std::move(new_col));
            new_names.push_back(name);
        }

        auto new_table = std::make_unique<cudf::table>(std::move(cols));
        return std::make_shared<GpuTable>(std::move(new_table), std::move(new_names));
    }

    // Serialize column names to a byte buffer (for inclusion in messages)
    std::vector<uint8_t> serialize_column_names() const
    {
        std::vector<uint8_t> buf;

        // Write number of columns
        uint32_t ncols = column_names_.size();
        auto* p = reinterpret_cast<const uint8_t*>(&ncols);
        buf.insert(buf.end(), p, p + sizeof(uint32_t));

        // Write each name as length-prefixed string
        for (auto const& name : column_names_)
        {
            uint32_t len = name.size();
            auto* lp = reinterpret_cast<const uint8_t*>(&len);
            buf.insert(buf.end(), lp, lp + sizeof(uint32_t));
            buf.insert(buf.end(), name.begin(), name.end());
        }

        return buf;
    }

    // Deserialize column names from a byte buffer
    static std::vector<std::string> deserialize_column_names(const uint8_t* data)
    {
        const uint8_t* ptr = data;

        uint32_t ncols = *reinterpret_cast<const uint32_t*>(ptr);
        ptr += sizeof(uint32_t);

        std::vector<std::string> names;
        names.reserve(ncols);

        for (uint32_t i = 0; i < ncols; i++)
        {
            uint32_t len = *reinterpret_cast<const uint32_t*>(ptr);
            ptr += sizeof(uint32_t);
            names.emplace_back(reinterpret_cast<const char*>(ptr), len);
            ptr += len;
        }

        return names;
    }
};

#endif // USE_GPU
#endif // GPU_TABLE_H
