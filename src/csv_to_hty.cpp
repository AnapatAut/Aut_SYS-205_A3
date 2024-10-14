// CSV to HTY Converter
// Converts CSV files to a custom HTY format with metadata

#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include "../third_party/nlohmann/json.hpp"

using json = nlohmann::json;

// Represents a column in the CSV/HTY file
struct Column
{
    std::string name;
    std::string type;
    std::vector<int> data;
};


// Converts a CSV file to HTY format
// Input: Path to input CSV file, path to output HTY file
void convert_from_csv_to_hty(const std::string& csv_file_path, const std::string& hty_file_path)
{
    // Read CSV file
    std::ifstream csv_file(csv_file_path);
    if (!csv_file.is_open())
    {
        std::cerr << "Error opening CSV file" << std::endl;
        return;
    }

    std::vector<Column> columns;
    std::string line;
    int num_rows = 0;

    // Read header
    if (std::getline(csv_file, line))
    {
        std::istringstream iss(line);
        std::string column_name;
        while (std::getline(iss, column_name, ','))
        {
            columns.push_back({column_name, "", {}});
        }
    }

    // Read data and determine column types
    while (std::getline(csv_file, line))
    {
        std::istringstream iss(line);
        std::string value;
        int col_index = 0;
        while (std::getline(iss, value, ','))
        {
            if (columns[col_index].type.empty())
            {
                // Determine column type based on value
                if (value.find('.') != std::string::npos)
                    columns[col_index].type = "float";
                else if (value.find_first_not_of("0123456789-") == std::string::npos)
                    columns[col_index].type = "int";
                else
                    columns[col_index].type = "string";
            }

            // Store data as int (interpret float and string later if needed)
            if (columns[col_index].type == "float")
            {
                float f_value = std::stof(value);
                int int_representation;
                std::memcpy(&int_representation, &f_value, sizeof(int));
                columns[col_index].data.push_back(int_representation);
            }
            else if (columns[col_index].type == "int")
            {
                columns[col_index].data.push_back(std::stoi(value));
            }
            else
            { // string
                // Store each character as an int
                for (char c : value)
                    columns[col_index].data.push_back(static_cast<int>(c));
                columns[col_index].data.push_back(0); // Add null terminator
            }
            col_index++;
        }
        num_rows++;
    }

    csv_file.close();

    // Prepare metadata
    json metadata;
    metadata["num_rows"] = num_rows;
    metadata["num_groups"] = 1;  // All columns in one group for simplicity
    json group;
    group["num_columns"] = columns.size();
    group["offset"] = 0;
    json columns_metadata;
    for (const auto& col : columns)
    {
        columns_metadata.push_back({{"column_name", col.name}, {"column_type", col.type}});
    }
    group["columns"] = columns_metadata;
    metadata["groups"].push_back(group);

    // Write HTY file
    std::ofstream hty_file(hty_file_path, std::ios::binary);
    if (!hty_file.is_open())
    {
        std::cerr << "Error opening HTY file for writing" << std::endl;
        return;
    }

    // Write raw data
    for (const auto& col : columns)
    {
        for (int value : col.data)
        {
            hty_file.write(reinterpret_cast<const char*>(&value), sizeof(int));
        }
    }

    // Write metadata
    std::string metadata_str = metadata.dump();
    hty_file.write(metadata_str.c_str(), metadata_str.size());

    // Write metadata size
    int metadata_size = static_cast<int>(metadata_str.size());
    hty_file.write(reinterpret_cast<const char*>(&metadata_size), sizeof(int));

    hty_file.close();

    std::cout << "Conversion completed successfully." << std::endl;
}

int main(int argc, char* argv[])
{
    if (argc != 3)
    {
        std::cerr << "Usage: " << argv[0] << " <input_csv_file> <output_hty_file>" << std::endl;
        return 1;
    }

    std::string csv_file_path = argv[1];
    std::string hty_file_path = argv[2];

    try
    {
        // Convert CSV to HTY
        convert_from_csv_to_hty(csv_file_path, hty_file_path);
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error during conversion: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}