#include <vector>
#include <cstring>
#include <arrow/io/file.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/api.h>
#include <arrow/table.h>
#include <parquet/arrow/reader.h>
#include "utils.hpp"
#include "serialize.hpp"
#include "partition.decl.h"


class Partition : public CBase_Partition
{
private:
    int num_partitions;
    std::unordered_map<int, std::shared_ptr<arrow::Table>> tables;

public:
    Partition(int num_partitions_) : num_partitions(num_partitions_) {}

    Partition(CkMigrateMessage* m) {}

    ~Partition()
    {
        // delete tables?
    }

    //void add_column(int table_name, )

    void read_parquet(int table_name, std::string file_path)
    {
        std::shared_ptr<arrow::io::ReadableFile> input_file;
        input_file = arrow::io::ReadableFile::Open(file_path).ValueOrDie();

        // Create a ParquetFileReader instance
        std::unique_ptr<parquet::arrow::FileReader> reader;
        parquet::arrow::OpenFile(input_file, arrow::default_memory_pool(), &reader);

        // Get the file metadata
        std::shared_ptr<parquet::FileMetaData> file_metadata = reader->parquet_reader()->metadata();

        int num_rows = file_metadata->num_rows();
        int nrows_per_partition = num_rows / num_partitions;
        int start_row = nrows_per_partition * thisIndex;
        int nextra_rows = num_rows - num_partitions * nrows_per_partition;

        if (thisIndex < nextra_rows)
        {
            start_row += thisIndex;
            nrows_per_partition++;
        }
        else
        {
            start_row += nextra_rows;
        }

        int num_row_groups = file_metadata->num_row_groups();

        // Variables to keep track of rows read
        int64_t rows_read = 0;
        int64_t rows_to_read = nrows_per_partition;

        std::vector<std::shared_ptr<arrow::Table>> row_tables;
        for (int i = 0; i < num_row_groups && rows_to_read > 0; ++i) 
        {
            int64_t row_group_num_rows = file_metadata->RowGroup(i)->num_rows();

            if (start_row >= row_group_num_rows) 
            {
                // Skip this row group
                start_row -= row_group_num_rows;
                continue;
            }

            // Calculate how many rows to read from this row group
            int64_t rows_in_group = std::min(rows_to_read, row_group_num_rows - start_row);

            // Read the rows
            std::shared_ptr<arrow::Table> table;
            reader->ReadRowGroup(i, &table);
            std::shared_ptr<arrow::Table> sliced_table = table->Slice(start_row, rows_in_group);
            row_tables.push_back(sliced_table);

            // Update counters
            rows_read += table->num_rows();
            rows_to_read -= table->num_rows();
            start_row = 0;  // Reset start_row for subsequent row groups
        }

        std::shared_ptr<arrow::Table> combined = arrow::ConcatenateTables(row_tables).ValueOrDie();
        tables[table_name] = combined;

        CkPrintf("[%d] Read number of rows = %i\n", thisIndex, combined->num_rows());
    }
};

#include "partition.def.h"
