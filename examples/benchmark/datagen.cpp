#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/scalar.h>
#include <arrow/compute/api.h>
#include <parquet/arrow/reader.h>
#include <algorithm>
#include <random>
#include <chrono>

arrow::Status CreateUserTable(int num_rows, int num_procs) {
    // Create schema
    auto schema = arrow::schema({
        arrow::field("first_name", arrow::utf8()),
        arrow::field("last_name", arrow::utf8()),
        arrow::field("city", arrow::utf8()),
        arrow::field("user_id", arrow::int64())
    });

    for (int pe = 0; pe < num_procs; pe++) {
        // Create array builders
        arrow::StringBuilder first_name_builder;
        arrow::StringBuilder last_name_builder;
        arrow::StringBuilder city_builder;
        arrow::Int64Builder user_id_builder;

        // Sample data (modify with your actual data)
        std::vector<std::string> first_names;
        std::vector<std::string> last_names;
        std::vector<std::string> cities;
        std::vector<int64_t> user_ids;

        for (int i = 0; i < num_rows; i++)
        {
            first_names.push_back("A" + std::to_string(pe) + "P" + std::to_string(i));
            last_names.push_back("B" + std::to_string(pe) + "P" + std::to_string(i));
            cities.push_back("C" + std::to_string(i % 1000));
            user_ids.push_back(num_rows * pe + i);
        }

        // Append values to builders
        ARROW_RETURN_NOT_OK(first_name_builder.AppendValues(first_names));
        ARROW_RETURN_NOT_OK(last_name_builder.AppendValues(last_names));
        ARROW_RETURN_NOT_OK(city_builder.AppendValues(cities));
        ARROW_RETURN_NOT_OK(user_id_builder.AppendValues(user_ids));

        // Finish arrays
        std::shared_ptr<arrow::Array> first_names_array;
        std::shared_ptr<arrow::Array> last_names_array;
        std::shared_ptr<arrow::Array> cities_array;
        std::shared_ptr<arrow::Array> user_ids_array;
        
        ARROW_RETURN_NOT_OK(first_name_builder.Finish(&first_names_array));
        ARROW_RETURN_NOT_OK(last_name_builder.Finish(&last_names_array));
        ARROW_RETURN_NOT_OK(city_builder.Finish(&cities_array));
        ARROW_RETURN_NOT_OK(user_id_builder.Finish(&user_ids_array));

        // Create table
        auto table = arrow::Table::Make(schema, {first_names_array, last_names_array, cities_array, user_ids_array});

        // Set up Parquet writer properties
        std::shared_ptr<parquet::WriterProperties> props = parquet::WriterProperties::Builder()
            .compression(parquet::Compression::SNAPPY)
            ->build();

        // Create output file
        std::shared_ptr<arrow::io::FileOutputStream> outfile;
        ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("user_ids_" + std::to_string(pe) + ".parquet"));

        // Write table to Parquet
        PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
            *table,
            arrow::default_memory_pool(),
            outfile,
            table->num_rows(),
            props
        ));
    }

    return arrow::Status::OK();
}

arrow::Status CreateAgesTable(int num_rows, int num_procs) {
    // Create schema
    auto schema = arrow::schema({
        arrow::field("first_name", arrow::utf8()),
        arrow::field("last_name", arrow::utf8()),
        arrow::field("age", arrow::int32())
    });

    for (int pe = 0; pe < num_procs; pe++) {
        // Create array builders
        arrow::StringBuilder first_name_builder;
        arrow::StringBuilder last_name_builder;
        arrow::Int32Builder age_builder;

        // Sample data (modify with your actual data)
        std::vector<std::string> first_names;
        std::vector<std::string> last_names;
        std::vector<int> ages;
        std::vector<int> indices(num_rows);

        for (int i = 0; i < num_rows; i++)
            indices[i] = i;

        std::shuffle(indices.begin(), indices.end(), std::default_random_engine(1234));

        for (int i = 0; i < num_rows; i++)
        {
            int idx = indices[i];
            first_names.push_back("A" + std::to_string(idx));
            last_names.push_back("B" + std::to_string(idx));
            ages.push_back((i + 1) % 100);
        }

        // Append values to builders
        ARROW_RETURN_NOT_OK(first_name_builder.AppendValues(first_names));
        ARROW_RETURN_NOT_OK(last_name_builder.AppendValues(last_names));
        ARROW_RETURN_NOT_OK(age_builder.AppendValues(ages));

        // Finish arrays
        std::shared_ptr<arrow::Array> first_names_array;
        std::shared_ptr<arrow::Array> last_names_array;
        std::shared_ptr<arrow::Array> ages_array;
        
        ARROW_RETURN_NOT_OK(first_name_builder.Finish(&first_names_array));
        ARROW_RETURN_NOT_OK(last_name_builder.Finish(&last_names_array));
        ARROW_RETURN_NOT_OK(age_builder.Finish(&ages_array));

        // Create table
        auto table = arrow::Table::Make(schema, {first_names_array, last_names_array, ages_array});

        // Set up Parquet writer properties
        std::shared_ptr<parquet::WriterProperties> props = parquet::WriterProperties::Builder()
            .compression(parquet::Compression::SNAPPY)
            ->build();

        // Create output file
        std::shared_ptr<arrow::io::FileOutputStream> outfile;
        ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("ages_" + std::to_string(pe) + ".parquet"));

        // Write table to Parquet
        PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
            *table,
            arrow::default_memory_pool(),
            outfile,
            table->num_rows(),
            props
        ));
    }

    return arrow::Status::OK();
}

int main(int argc, char** argv) {
    int num_rows = std::atoi(argv[1]);
    int num_procs = std::atoi(argv[2]);
    arrow::Status status1 = CreateUserTable(num_rows, num_procs);
    arrow::Status status2 = CreateAgesTable(num_rows, num_procs);
}
