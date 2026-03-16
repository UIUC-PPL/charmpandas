#ifndef SERIALIZE_H
#define SERIALIZE_H

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/ipc/writer.h"
#include "utils.hpp"

#ifdef USE_GPU

#include "gpu_table.hpp"
#include <cudf/contiguous_split.hpp>
#include <cudf/interop.hpp>

// ============================================================================
// GPU-native serialization for Charm++ inter-node communication (RDMA)
// ============================================================================

// Packed representation for GPU-Direct RDMA transfer
struct PackedTableData
{
    std::vector<uint8_t> metadata;        // host: cudf packed_columns metadata
    std::vector<uint8_t> col_names_buf;   // host: serialized column names
    rmm::device_buffer gpu_data;          // device: contiguous table data (sent via RDMA)

    // Total host metadata size (for message allocation)
    size_t host_metadata_size() const
    {
        return sizeof(uint32_t)                // metadata size
             + metadata.size()                 // cudf metadata
             + sizeof(uint32_t)                // col_names_buf size
             + col_names_buf.size();           // column names
    }

    // Serialize host-side metadata into a contiguous buffer
    std::vector<uint8_t> serialize_host_metadata() const
    {
        std::vector<uint8_t> buf;
        buf.reserve(host_metadata_size());

        // Write cudf metadata
        uint32_t meta_size = metadata.size();
        auto* p = reinterpret_cast<const uint8_t*>(&meta_size);
        buf.insert(buf.end(), p, p + sizeof(uint32_t));
        buf.insert(buf.end(), metadata.begin(), metadata.end());

        // Write column names
        uint32_t names_size = col_names_buf.size();
        auto* np = reinterpret_cast<const uint8_t*>(&names_size);
        buf.insert(buf.end(), np, np + sizeof(uint32_t));
        buf.insert(buf.end(), col_names_buf.begin(), col_names_buf.end());

        return buf;
    }

    // Deserialize host-side metadata from a buffer
    static void deserialize_host_metadata(const uint8_t* data,
        std::vector<uint8_t>& out_metadata,
        std::vector<std::string>& out_col_names)
    {
        const uint8_t* ptr = data;

        // Read cudf metadata
        uint32_t meta_size = *reinterpret_cast<const uint32_t*>(ptr);
        ptr += sizeof(uint32_t);
        out_metadata.assign(ptr, ptr + meta_size);
        ptr += meta_size;

        // Read column names
        ptr += sizeof(uint32_t); // skip names_size (not needed, deserialize reads it)
        out_col_names = GpuTable::deserialize_column_names(ptr);
    }
};

// Pack a GpuTable for RDMA transfer
inline PackedTableData gpu_pack(TablePtr& table)
{
    PackedTableData packed;

    // cudf::pack produces contiguous device buffer + host metadata
    auto cudf_packed = cudf::pack(table->view());

    packed.metadata = std::move(*cudf_packed.metadata);
    packed.gpu_data = std::move(*cudf_packed.gpu_data);
    packed.col_names_buf = table->serialize_column_names();

    return packed;
}

// Unpack a GpuTable from RDMA-received data
inline TablePtr gpu_unpack(const uint8_t* host_metadata,
    const uint8_t* device_data, size_t device_size)
{
    std::vector<uint8_t> cudf_metadata;
    std::vector<std::string> col_names;
    PackedTableData::deserialize_host_metadata(host_metadata,
        cudf_metadata, col_names);

    return GpuTable::unpack(cudf_metadata.data(), cudf_metadata.size(),
        device_data, device_size, std::move(col_names));
}

// ============================================================================
// Arrow IPC serialization — used ONLY for Python fetch (CCS reply to host)
// ============================================================================

inline arrow::Status serialize_for_host(TablePtr& table, BufferPtr& out)
{
    // GPU -> Arrow table (D2H transfer)
    auto arrow_table = table->to_arrow();

    // Standard Arrow IPC serialization
    ARROW_ASSIGN_OR_RAISE(auto output_stream, arrow::io::BufferOutputStream::Create());
    ARROW_ASSIGN_OR_RAISE(auto writer,
        arrow::ipc::MakeStreamWriter(output_stream, arrow_table->schema()));
    ARROW_RETURN_NOT_OK(writer->WriteTable(*arrow_table));
    ARROW_RETURN_NOT_OK(writer->Close());
    ARROW_RETURN_NOT_OK(output_stream->Close());
    ARROW_ASSIGN_OR_RAISE(out, output_stream->Finish());
    return arrow::Status::OK();
}

// serialize() — used internally by messaging code. Under GPU, falls back to
// Arrow IPC via host for compatibility with existing message types.
// For RDMA transfers, use gpu_pack() directly instead.
inline arrow::Status serialize(TablePtr& table, BufferPtr& out)
{
    return serialize_for_host(table, out);
}

// Deserialize Arrow IPC bytes into a GpuTable (for receiving from Python/CPU sources)
inline TablePtr deserialize(char* data, int size)
{
    BufferPtr buffer = arrow::Buffer::Wrap(data, size);
    auto input = std::make_shared<arrow::io::BufferReader>(buffer);
    auto reader = arrow::ipc::RecordBatchStreamReader::Open(input).ValueOrDie();
    auto arrow_table = reader->ToTable().ValueOrDie();
    return GpuTable::from_arrow(arrow_table);
}

#else // !USE_GPU

// ============================================================================
// CPU-only Arrow IPC serialization (original implementation)
// ============================================================================

arrow::Status serialize(TablePtr &table, BufferPtr &out)
{
    ARROW_ASSIGN_OR_RAISE(auto output_stream, arrow::io::BufferOutputStream::Create());
    ARROW_ASSIGN_OR_RAISE(auto writer,
        arrow::ipc::MakeStreamWriter(output_stream, table->schema()));
    ARROW_RETURN_NOT_OK(writer->WriteTable(*table));
    ARROW_RETURN_NOT_OK(writer->Close());
    ARROW_RETURN_NOT_OK(output_stream->Close());
    ARROW_ASSIGN_OR_RAISE(out, output_stream->Finish());
    return arrow::Status::OK();
}

TablePtr deserialize(char* data, int size)
{
    BufferPtr buffer = arrow::Buffer::Wrap(data, size);
    auto input = std::make_shared<arrow::io::BufferReader>(buffer);
    auto reader = arrow::ipc::RecordBatchStreamReader::Open(input).ValueOrDie();
    TablePtr table = reader->ToTable().ValueOrDie();
    return table;
}

#endif // USE_GPU

#endif // SERIALIZE_H
