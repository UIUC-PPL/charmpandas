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

    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> reader = 
        arrow::ipc::RecordBatchStreamReader::Open(input).ValueOrDie();

    // Create table from batches
    std::shared_ptr<arrow::Table> table = reader->ToTable().ValueOrDie();

    return table;
}