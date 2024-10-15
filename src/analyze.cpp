#include <cstdarg>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>
#include "../third_party/nlohmann/json.hpp"

using json = nlohmann::json;

void print_info(int status, const char* function_name);
void print_debug(const std::string& format, ...);

// Extracts metadata from an HTY file
// Input: Path to HTY file
// Output: JSON object containing metadata
json extract_metadata(std::string hty_file_path)
{
    print_info(0, __func__);
    std::ifstream file(hty_file_path, std::ios::binary | std::ios::ate);
    if (!file.is_open())
    {
        throw std::runtime_error("Unable to open file");
    }

    // Get file size and read metadata size
    std::streamsize size = file.tellg();
    file.seekg(size - sizeof(int));
    int metadata_size;
    file.read(reinterpret_cast<char*>(&metadata_size), sizeof(int));

    // Read and parse metadata
    file.seekg(size - metadata_size - sizeof(int));
    std::string metadata_str(metadata_size, '\0');
    file.read(&metadata_str[0], metadata_size);
    json metadata = json::parse(metadata_str);

    // Print metadata for debugging
    std::cout << "Metadata contents:" << std::endl;
    std::cout << metadata.dump(2) << std::endl;

    print_info(1, __func__);
    return metadata;
}

// Projects a single column from an HTY file
// Input: Metadata, HTY file path, column name
// Output: Vector of integers representing the column data
std::vector<int> project_single_column(json metadata, std::string hty_file_path, std::string projected_column)
{
    print_info(0, __func__);
    std::ifstream file(hty_file_path, std::ios::binary);
    if (!file.is_open())
    {
        throw std::runtime_error("Unable to open file");
    }

    int num_rows = metadata["num_rows"].get<int>();
    std::vector<int> result;

    // Find the column and read its data
    for (const auto& group : metadata["groups"])
    {
        for (size_t i = 0; i < group["columns"].size(); ++i)
        {
            if (group["columns"][i]["column_name"] == projected_column)
            {
                // Calculate offset and read data
                int64_t base_offset = group["offset"].get<int64_t>();
                int64_t column_offset = static_cast<int64_t>(i) * static_cast<int64_t>(num_rows) * sizeof(int);
                int64_t offset = base_offset + column_offset;
                
                file.seekg(offset, std::ios::beg);
                result.resize(num_rows);
                file.read(reinterpret_cast<char*>(result.data()), num_rows * sizeof(int));
                print_info(1, __func__);
                return result;
            }
        }
    }  
    throw std::runtime_error("Column not found");
}

// Displays a column's data
// Input: Metadata, column name, column data
void display_column(json metadata, std::string column_name, std::vector<int> data)
{
    print_info(0, __func__);
    std::cout << column_name << std::endl;
    std::string column_type;

    // Find column type
    for (const auto& group : metadata["groups"])
    {
        for (const auto& column : group["columns"])
        {
            if (column["column_name"] == column_name)
            {
                column_type = column["column_type"].get<std::string>();
                break;
            }
        }
        if (!column_type.empty()) break;
    }

    // Print data based on column type
    for (const auto& value : data)
    {
        if (column_type == "float")
        {
            float float_value = *reinterpret_cast<const float*>(&value);
            std::cout << float_value << std::endl;
        }
        else
        {
            std::cout << value << std::endl;
        }
    }
    print_info(1, __func__);
}

// Filters data based on a condition
// Input: Metadata, HTY file path, column to filter, operation, filter value
// Output: Vector of indices meeting the filter condition
std::vector<int> filter(json metadata, std::string hty_file_path, std::string filtered_column, int operation, float filtered_value)
{
    print_info(0 , __func__);
    std::vector<int> column_data = project_single_column(metadata, hty_file_path, filtered_column);
    print_debug("Column data size: %zu\n", column_data.size());
    std::vector<int> result;

    // Get column type
    std::string column_type;
    for (const auto& group : metadata["groups"])
    {
        for (const auto& column : group["columns"])
        {
            if (column["column_name"] == filtered_column)
            {
                column_type = column["column_type"].get<std::string>();
                break;
            }
        }
        if (!column_type.empty()) break;
    }
    print_debug("Column type: %s\n", column_type.c_str());

    // Define comparison lambda
    auto compare = [operation, column_type](int a, float b)
    {
        float a_float;
        if (column_type == "float")
        {
            std::memcpy(&a_float, &a, sizeof(float));
        }
        else
        {
            a_float = static_cast<float>(a);
        }
        switch (operation)
        {
            case 0: return a_float > b;
            case 1: return a_float >= b;
            case 2: return a_float < b;
            case 3: return a_float <= b;
            case 4: return std::abs(a_float - b) < 1e-6;
            case 5: return std::abs(a_float - b) >= 1e-6;
            default: throw std::runtime_error("Invalid operation");
        }
    };

    // Apply filter
    for (size_t i = 0; i < column_data.size(); ++i)
    {
        if (compare(column_data[i], filtered_value))
        {
            result.push_back(i);
        }
    }

    print_debug("Filter result size: %zu\n", result.size());
    print_info(1 , __func__);
    return result;
}

// Projects multiple columns from an HTY file
// Input: Metadata, HTY file path, list of column names
// Output: Vector of vectors containing projected data
std::vector<std::vector<int>> project(json metadata, std::string hty_file_path, std::vector<std::string> projected_columns)
{
    print_info(0, __func__);
    std::ifstream file(hty_file_path, std::ios::binary);
    if (!file.is_open())
    {
        throw std::runtime_error("Unable to open file");
    }

    int num_rows = metadata["num_rows"].get<int>();
    print_debug("Number of rows: %d\n", num_rows);
    std::vector<std::vector<int>> result(projected_columns.size(), std::vector<int>(num_rows));

    for (const auto& group : metadata["groups"])
    {
        std::vector<size_t> column_indices;
        // Find indices of projected columns
        for (const auto& projected_column : projected_columns)
        {
            bool found = false;
            for (size_t i = 0; i < group["columns"].size(); ++i)
            {
                if (group["columns"][i]["column_name"] == projected_column)
                {
                    column_indices.push_back(i);
                    found = true;
                    break;
                }
            }
            if (!found)
            {
                print_debug("Column not found: %s\n", projected_column.c_str());
            }
        }

        if (column_indices.size() == projected_columns.size())
        {
            // Read data for each column
            int64_t base_offset = group["offset"].get<int64_t>();
            for (size_t i = 0; i < column_indices.size(); ++i)
            {
                int64_t column_offset = static_cast<int64_t>(column_indices[i]) * static_cast<int64_t>(num_rows) * sizeof(int);
                std::string column_type = group["columns"][column_indices[i]]["column_type"].get<std::string>();

                size_t data_size = (column_type == "float") ? sizeof(float) : sizeof(int);
                int64_t offset = base_offset + column_offset;
                
                print_debug("Reading column %s from offset %d\n", projected_columns[i].c_str(), offset);
                file.seekg(offset, std::ios::beg);
                if (!file)
                {
                    throw std::runtime_error("Failed to seek to offset " + std::to_string(offset));
                }

                file.read(reinterpret_cast<char*>(result[i].data()), num_rows * sizeof(int));

                if (!file)
                {
                    throw std::runtime_error("Failed to read data for column " + projected_columns[i]);
                }
            }

            print_debug("Project result size: %zu x %zu\n", result.size(), result.empty() ? 0 : result[0].size());
            print_info(1, __func__);
            return result;
        }
    }

    throw std::runtime_error("Columns not found in the same group");
}

// Projects and filters data
// Input: Metadata, HTY file path, columns to project, filter column, operation, filter value
// Output: Vector of vectors containing filtered and projected data
std::vector<std::vector<int>> project_and_filter(json metadata, std::string hty_file_path, std::vector<std::string> projected_columns, std::string filtered_column, int op, float value)
{
    print_info(0, __func__);
    std::string columns_str;
    for (const auto& col : projected_columns)
    {
        columns_str += col + " ";
    }
    print_debug("Projected columns: %s", columns_str.c_str());

    std::cout << std::endl;
    print_debug("Filtered column: %s\n", filtered_column.c_str());
    print_debug("Operation: %d, Value: %d\n", op, value);

    // Retrieve all data based on projection
    std::vector<std::vector<int>> all_data = project(metadata, hty_file_path, projected_columns);

    // Filter indices based on the provided criteria
    std::vector<int> filtered_indices = filter(metadata, hty_file_path, filtered_column, op, value);

    // Apply filter to projected data
    std::vector<std::vector<int>> result;
    for (size_t col = 0; col < all_data.size(); ++col)
    { 
        std::vector<int> row;
        for (const auto& index : filtered_indices)
        {
            if (index >= all_data[0].size())
            {
                print_debug("Index out of range: %d\n", index);
                continue;
            }
            int value = all_data[col][index];
            row.push_back(value);
        }
        result.push_back(row);
    }

    print_info(1, __func__);
    return result;
}

// Displays a result set
// Input: Metadata, column names, result set data
void display_result_set(json metadata, std::vector<std::string> column_names, std::vector<std::vector<int>> result_set)
{
    print_info(0, __func__);
    const int column_width = 10;
    print_debug("Result set size: %zu x %zu\n", result_set.size(), result_set.empty() ? 0 : result_set[0].size());

    // Check the contents of result_set
    for (size_t row = 0; row < result_set.size(); ++row)
    {
        std::ostringstream contents_stream;
        for (const auto& value : result_set[row])
        {
            contents_stream << value << " ";
        }
        print_debug("Row %d size: %zu contents: %s\n", row, result_set[row].size(), contents_stream.str().c_str());
    }

    if (result_set.empty())
    {
        std::cout << "No results to display." << std::endl;
        return;
    }

    // Get column types
    std::vector<std::string> column_types;
    for (const auto& column_name : column_names)
    {
        bool found = false;
        for (const auto& group : metadata["groups"])
        {
            for (const auto& column : group["columns"])
            {
                if (column["column_name"] == column_name)
                {
                    column_types.push_back(column["column_type"].get<std::string>());
                    found = true;
                    break;
                }
            }
            if (found) break;
        }
        if (!found)
        {
            print_debug("Column type not found for %s\n", column_name.c_str());
            column_types.push_back("unknown");
        }
    }

    // Print header
    for (size_t i = 0; i < column_names.size(); ++i)
    {
        std::cout << std::setw(column_width) << std::left << column_names[i];
    }
    std::cout << std::endl;

    // Print data
    for (size_t row = 0; row < result_set[0].size(); ++row)
    {
        for (size_t col = 0; col < result_set.size(); ++col)
        {
            // Print each value based on its type
            const auto& value = result_set[col][row];
            if (column_types[col] == "float")
            {
                float float_value = *reinterpret_cast<const float*>(&value);
                std::cout << std::setw(column_width) << std::left << float_value;
            }
            else
            {
                std::cout << std::setw(column_width) << std::left << value;
            }
        }
        std::cout << std::endl; // Newline after each row
    }
    print_info(1, __func__);
}

// Adds new rows to an HTY file
// Input: Metadata, original HTY file path, new HTY file path, new rows data
void add_row(json metadata, std::string hty_file_path, std::string modified_hty_file_path, std::vector<std::vector<int>> rows)
{
    // Read existing data
    std::vector<std::vector<int>> existing_data;
    for (const auto& group : metadata["groups"])
    {
        for (const auto& column : group["columns"])
        {
            existing_data.push_back(project_single_column(metadata, hty_file_path, column["column_name"].get<std::string>()));
        }
    }

    // Update metadata with new row count
    metadata["num_rows"] = metadata["num_rows"].get<int>() + rows.size();

    // Write modified .hty file
    std::ofstream out_file(modified_hty_file_path, std::ios::binary);
    if (!out_file.is_open())
    {
        throw std::runtime_error("Unable to open output file");
    }

    // Write raw data (existing + new rows)
    for (size_t i = 0; i < existing_data.size(); ++i)
    {
        // Write existing data
        for (const auto& value : existing_data[i])
        {
            out_file.write(reinterpret_cast<const char*>(&value), sizeof(int));
        }
        // Write new row data
        for (const auto& row : rows)
        {
            out_file.write(reinterpret_cast<const char*>(&row[i]), sizeof(int));
        }
    }

    // Write updated metadata
    std::string metadata_str = metadata.dump();
    out_file.write(metadata_str.c_str(), metadata_str.size());

    // Write metadata size
    int metadata_size = static_cast<int>(metadata_str.size());
    out_file.write(reinterpret_cast<const char*>(&metadata_size), sizeof(int));

    out_file.close();
}

// Converts operation code to string representation
std::string operation_to_string(int op)
{
    switch (op)
    {
        case 0: return ">";
        case 1: return ">=";
        case 2: return "<";
        case 3: return "<=";
        case 4: return "=";
        case 5: return "!=";
        default: return "unknown";
    }
}

// Prints function entry/exit information
void print_info(int status, const char* function_name)
{
    const std::string bright_cyan = "\033[1;36m";
    const std::string reset = "\033[0m";

    std::string msg = (status == 0) ? "Entering" : (status == 1) ? "Exiting" : "Unknown status";

    std::cout << bright_cyan << "[i] " << msg << " " << function_name << " function" << reset << std::endl;
}

// Prints debug information
void print_debug(const std::string& format, ...)
{
    const std::string bright_yellow = "\033[1;33m";
    const std::string reset = "\033[0m";

    // Prepare the formatted message
    va_list args;
    va_start(args, format);
    char buffer[256];
    vsnprintf(buffer, sizeof(buffer), format.c_str(), args);
    va_end(args);

    std::cout << bright_yellow << "Debug: " << buffer << reset;
}

// Main function: demonstrates usage of HTY file operations
int main()
{
    try
    {
        std::string hty_file_path = "test/test.hty";
        
        // Test extract_metadata
        std::cout << std::endl << "----------Metadata----------" << std::endl;
        json metadata = extract_metadata(hty_file_path);

        // Test project_single_column and display_column
        std::cout << std::endl << "----------Single column----------" << std::endl;
        std::string column_name = "salary";
        std::vector<int> column_data = project_single_column(metadata, hty_file_path, column_name);
        display_column(metadata, column_name, column_data);

        // Test project and display_result_set for all columns
        std::cout << std::endl << "----------All Columns----------" << std::endl;
        std::vector<std::string> all_columns;
        for (const auto& group : metadata["groups"])
        {
            for (const auto& column : group["columns"])
            {
                all_columns.push_back(column["column_name"].get<std::string>());
            }
        }
        std::vector<std::vector<int>> all_data = project(metadata, hty_file_path, all_columns);
        display_result_set(metadata, all_columns, all_data);

        // Test filter
        std::cout << std::endl << "----------Filter----------" << std::endl;
        std::string filter_column = "salary";
        float filter_value = 50000.0f;
        int filter_op = 2; // Less than
        std::vector<int> unfiltered_data = project_single_column(metadata, hty_file_path, filter_column);
        std::vector<int> filtered_indices = filter(metadata, hty_file_path, filter_column, filter_op, filter_value);
        std::cout << "Filtered indices (" << filter_column << " " << operation_to_string(filter_op) << " " << filter_value << "): ";
        for (const auto& index : filtered_indices)
        {
            std::cout << index << " ";
        }
        std::cout << unfiltered_data.size() << std::endl;
        std::vector<int> result;
        for (const auto& index : filtered_indices)
        {
            if (index >= unfiltered_data.size())
            {
                print_debug("Index out of range: %d\n", index);
                continue;
            }
            int value = unfiltered_data[index];
            result.push_back(value);
        }
        display_column(metadata, filter_column, result);
        std::cout << std::endl;

        // Test project with specific columns
        std::cout << "----------Project----------" << std::endl;
        std::vector<std::string> projected_columns = {"id", "salary"};
        std::vector<std::vector<int>> projected_data = project(metadata, hty_file_path, projected_columns);
        std::cout << "Projected data size: " << projected_data.size() << " x " << projected_data[0].size() << std::endl;
        display_result_set(metadata, projected_columns, projected_data);

        // Test project_and_filter
        std::cout << std::endl << "----------Project and Filter----------" << std::endl;
        filter_column = "salary";
        filter_value = 50000.0f;
        filter_op = 2; // Less than
        std::vector<std::vector<int>> filtered_data = project_and_filter(metadata, hty_file_path, all_columns, filter_column, filter_op, filter_value);
        std::cout << "Filtered data (" << filter_column << " " << operation_to_string(filter_op) << " " << filter_value << "):" << std::endl;
        display_result_set(metadata, all_columns, filtered_data);
        std::cout << std::endl;

        // Test add_row
        std::cout << "----------Add row----------" << std::endl;
        std::vector<std::vector<int>> new_rows = {
            {7, 20, [] { float temp = 90000.3f; return *reinterpret_cast<const int*>(&temp); }(), 
                       [] { float temp = 3.1f; return *reinterpret_cast<const int*>(&temp); }()},
            {8, 31, [] { float temp = 32000.2f; return *reinterpret_cast<const int*>(&temp); }(), 
                       [] { float temp = 2.9f; return *reinterpret_cast<const int*>(&temp); }()},
            {9, 24, [] { float temp = 85000.8f; return *reinterpret_cast<const int*>(&temp); }(), 
                       [] { float temp = 4.6f; return *reinterpret_cast<const int*>(&temp); }()}
        };
        std::string modified_hty_file_path = "test/modified_test.hty";
        add_row(metadata, hty_file_path, modified_hty_file_path, new_rows);

        // Verify new data
        json modified_metadata = extract_metadata(modified_hty_file_path);
        int expected_rows = metadata["num_rows"].get<int>() + new_rows.size();
        int actual_rows = modified_metadata["num_rows"].get<int>();
        print_debug("Expected rows: %d, Actual rows: %d\n", expected_rows, actual_rows);
        assert(actual_rows == expected_rows && "Number of rows mismatch");

        // Display original and modified data
        all_columns.clear();
        for (const auto& group : modified_metadata["groups"])
        {
            for (const auto& column : group["columns"])
            {
                all_columns.push_back(column["column_name"].get<std::string>());
            }
        }
        std::cout << "\nOriginal data:\n";
        std::vector<std::vector<int>> original_data = project(metadata, hty_file_path, all_columns);
        display_result_set(metadata, all_columns, original_data);
        std::cout << "\nModified data:\n";
        std::vector<std::vector<int>> modified_data = project(modified_metadata, modified_hty_file_path, all_columns);
        display_result_set(modified_metadata, all_columns, modified_data);

        // Verify data integrity
        print_debug("Verifying new data\n");
        for (size_t i = 0; i < all_columns.size(); ++i)
        {
            std::string column_name = all_columns[i];
            // Check if original data is preserved
            assert(std::equal(original_data[i].begin(), original_data[i].end(), modified_data[i].begin()) && 
                   "Original data not preserved");
            // Check if new rows are correctly added
            for (size_t j = 0; j < new_rows.size(); ++j)
            {
                int expected_value = new_rows[j][i];
                int actual_value = modified_data[i][original_data[i].size() + j];
                print_debug("Column %s, New row %zu, Expected: %d, Actual: %d\n", column_name.c_str(), j, expected_value, actual_value);
                assert(expected_value == actual_value && "New row data mismatch");
            }
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    std::cout << std::endl << "\033[1;32mAll tests completed!\033[0m" << std::endl;
    return 0;
}