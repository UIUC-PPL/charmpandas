#include <stdexcept>
#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/ipc/writer.h"

arrow::Status serialize(std::shared_ptr<arrow::Table> &table, std::shared_ptr<arrow::Buffer> &out)
{
    //std::shared_ptr<arrow::Buffer> out;
    // Create output stream
    ARROW_ASSIGN_OR_RAISE(auto output_stream, arrow::io::BufferOutputStream::Create());

    // Create writer
    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(output_stream, table->schema()));

    // Write table
    ARROW_RETURN_NOT_OK(writer->WriteTable(*table));

    // Close writer
    ARROW_RETURN_NOT_OK(writer->Close());

    // Close output stream
    ARROW_RETURN_NOT_OK(output_stream->Close());

    // For BufferOutputStream, use Finish() to get the buffer
    ARROW_ASSIGN_OR_RAISE(out, output_stream->Finish());

    return arrow::Status::OK();
}

std::shared_ptr<arrow::Table> deserialize(char* data, int size)
{
    std::shared_ptr<arrow::Buffer> buffer = arrow::Buffer::Wrap(data, size);

    std::shared_ptr<arrow::io::BufferReader> input = std::make_shared<arrow::io::BufferReader>(buffer);

    arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchStreamReader>> maybe_reader = 
        arrow::ipc::RecordBatchStreamReader::Open(input);

    if (!maybe_reader.ok()) {
        // Handle error
        std::runtime_error("Failed opening buffer reader during deserialization");
        return nullptr;
    }

    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> reader = *maybe_reader;

    // Read all record batches
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch;
    while ((maybe_batch = reader->Next()).ok()) {
        batches.push_back(*maybe_batch);
    }

    // Create table from batches
    arrow::Result<std::shared_ptr<arrow::Table>> maybe_table = arrow::Table::FromRecordBatches(batches);

    if (!maybe_table.ok()) {
        // Handle error
        std::runtime_error("Table deserialization failed");
        return nullptr;
    }

    return *maybe_table;
}