#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cstring>
#include <cassert>
#include <cmath>
#include <algorithm>
#include "../third_party/nlohmann/json.hpp"

using json = nlohmann::json;

json extract_metadata(std::string hty_file_path) {
    std::ifstream file(hty_file_path, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        throw std::runtime_error("Unable to open file");
    }

    std::streamsize size = file.tellg();
    file.seekg(size - sizeof(int));

    int metadata_size;
    file.read(reinterpret_cast<char*>(&metadata_size), sizeof(int));

    file.seekg(size - metadata_size - sizeof(int));
    std::string metadata_str(metadata_size, '\0');
    file.read(&metadata_str[0], metadata_size);

    return json::parse(metadata_str);
}

std::vector<int> project_single_column(json metadata, std::string hty_file_path, std::string projected_column) {
    std::ifstream file(hty_file_path, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Unable to open file");
    }

    int num_rows = metadata["num_rows"].get<int>();
    std::vector<int> result;

    for (const auto& group : metadata["groups"]) {
        for (size_t i = 0; i < group["columns"].size(); ++i) {
            if (group["columns"][i]["column_name"] == projected_column) {
                int64_t base_offset = group["offset"].get<int64_t>();
                int64_t column_offset = static_cast<int64_t>(i) * static_cast<int64_t>(num_rows) * sizeof(int);
                int64_t offset = base_offset + column_offset;
                
                file.seekg(offset, std::ios::beg);
                result.resize(num_rows);
                file.read(reinterpret_cast<char*>(result.data()), num_rows * sizeof(int));
                return result;
            }
        }
    }

    throw std::runtime_error("Column not found");
}

void display_column(json metadata, std::string column_name, std::vector<int> data) {
    std::cout << column_name << std::endl;
    std::string column_type;

    for (const auto& group : metadata["groups"]) {
        for (const auto& column : group["columns"]) {
            if (column["column_name"] == column_name) {
                column_type = column["column_type"].get<std::string>();
                break;
            }
        }
        if (!column_type.empty()) break;
    }

    for (const auto& value : data) {
        if (column_type == "float") {
            float float_value;
            std::memcpy(&float_value, &value, sizeof(float));
            std::cout << float_value << std::endl;
        } else {
            std::cout << value << std::endl;
        }
    }
}

std::vector<int> filter(json metadata, std::string hty_file_path, std::string filtered_column, int operation, float filtered_value) {
    std::vector<int> column_data = project_single_column(metadata, hty_file_path, filtered_column);
    std::vector<int> result;

    std::string column_type;
    for (const auto& group : metadata["groups"]) {
        for (const auto& column : group["columns"]) {
            if (column["column_name"] == filtered_column) {
                column_type = column["column_type"].get<std::string>();
                break;
            }
        }
        if (!column_type.empty()) break;
    }

    auto compare = [operation, column_type](int a, float b) {
        float a_float;
        if (column_type == "float") {
            std::memcpy(&a_float, &a, sizeof(float));
        } else {
            a_float = static_cast<float>(a);
        }
        switch (operation) {
            case 0: return a_float > b;
            case 1: return a_float >= b;
            case 2: return a_float < b;
            case 3: return a_float <= b;
            case 4: return std::abs(a_float - b) < 1e-6;  // Use epsilon comparison for floats
            case 5: return std::abs(a_float - b) >= 1e-6;
            default: throw std::runtime_error("Invalid operation");
        }
    };

    for (size_t i = 0; i < column_data.size(); ++i) {
        if (compare(column_data[i], filtered_value)) {
            result.push_back(i);
        }
    }

    return result;
}

std::vector<std::vector<int>> project(json metadata, std::string hty_file_path, std::vector<std::string> projected_columns) {
    std::ifstream file(hty_file_path, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Unable to open file");
    }

    int num_rows = metadata["num_rows"].get<int>();
    std::vector<std::vector<int>> result(projected_columns.size(), std::vector<int>(num_rows));

    for (const auto& group : metadata["groups"]) {
        std::vector<size_t> column_indices;
        for (const auto& projected_column : projected_columns) {
            bool found = false;
            for (size_t i = 0; i < group["columns"].size(); ++i) {
                if (group["columns"][i]["column_name"] == projected_column) {
                    column_indices.push_back(i);
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw std::runtime_error("Column not found in the same group");
            }
        }

        int64_t base_offset = group["offset"].get<int64_t>();
        for (size_t i = 0; i < column_indices.size(); ++i) {
            int64_t column_offset = static_cast<int64_t>(column_indices[i]) * static_cast<int64_t>(num_rows) * sizeof(int);
            int64_t offset = base_offset + column_offset;
            
            file.seekg(offset, std::ios::beg);
            file.read(reinterpret_cast<char*>(result[i].data()), num_rows * sizeof(int));
        }

        return result;
    }

    throw std::runtime_error("Columns not found in the same group");
}

void display_result_set(json metadata, std::vector<std::string> column_names, std::vector<std::vector<int>> result_set) {
    if (result_set.empty()) {
        std::cout << "No results to display." << std::endl;
        return;
    }

    std::vector<std::string> column_types;
    for (const auto& column_name : column_names) {
        for (const auto& group : metadata["groups"]) {
            for (const auto& column : group["columns"]) {
                if (column["column_name"] == column_name) {
                    column_types.push_back(column["column_type"].get<std::string>());
                    break;
                }
            }
            if (column_types.size() == column_names.size()) break;
        }
    }

    // Print header
    for (size_t i = 0; i < column_names.size(); ++i) {
        std::cout << column_names[i];
        if (i < column_names.size() - 1) std::cout << ",";
    }
    std::cout << std::endl;

    // Print data
    for (const auto& row : result_set) {
        for (size_t i = 0; i < row.size(); ++i) {
            if (column_types[i] == "float") {
                float float_value;
                std::memcpy(&float_value, &row[i], sizeof(float));
                std::cout << std::fixed << std::setprecision(2) << float_value;
            } else {
                std::cout << row[i];
            }
            if (i < row.size() - 1) std::cout << ",";
        }
        std::cout << std::endl;
    }
}

std::vector<std::vector<int>> project_and_filter(json metadata, std::string hty_file_path, std::vector<std::string> projected_columns, std::string filtered_column, int op, int value) {
    // Find the group containing all required columns
    json target_group;
    for (const auto& group : metadata["groups"]) {
        std::set<std::string> group_columns;
        for (const auto& col : group["columns"]) {
            group_columns.insert(col["column_name"]);
        }
        if (std::includes(group_columns.begin(), group_columns.end(), 
                          projected_columns.begin(), projected_columns.end()) &&
            group_columns.find(filtered_column) != group_columns.end()) {
            target_group = group;
            break;
        }
    }
    if (target_group.is_null()) {
        throw std::runtime_error("Columns not found in the same group");
    }

    std::vector<std::vector<int>> all_data = project(metadata, hty_file_path, projected_columns);
    std::vector<int> filtered_indices = filter(metadata, hty_file_path, filtered_column, op, value);
    std::vector<std::vector<int>> result;

    for (const auto& index : filtered_indices) {
        std::vector<int> row;
        for (const auto& col : all_data) {
            row.push_back(col[index]);
        }
        result.push_back(row);
    }

    return result;
}

void add_row(json metadata, std::string hty_file_path, std::string modified_hty_file_path, std::vector<std::vector<int>> rows) {
    // Read existing data
    std::vector<std::vector<int>> existing_data;
    for (const auto& group : metadata["groups"]) {
        for (const auto& column : group["columns"]) {
            existing_data.push_back(project_single_column(metadata, hty_file_path, column["column_name"].get<std::string>()));
        }
    }

    // Update metadata
    metadata["num_rows"] = metadata["num_rows"].get<int>() + rows.size();

    // Write modified .hty file
    std::ofstream out_file(modified_hty_file_path, std::ios::binary);
    if (!out_file.is_open()) {
        throw std::runtime_error("Unable to open output file");
    }

    // Write raw data
    for (size_t i = 0; i < existing_data.size(); ++i) {
        for (const auto& value : existing_data[i]) {
            out_file.write(reinterpret_cast<const char*>(&value), sizeof(int));
        }
        for (const auto& row : rows) {
            out_file.write(reinterpret_cast<const char*>(&row[i]), sizeof(int));
        }
    }

    // Write metadata
    std::string metadata_str = metadata.dump();
    out_file.write(metadata_str.c_str(), metadata_str.size());

    // Write metadata size
    int metadata_size = static_cast<int>(metadata_str.size());
    out_file.write(reinterpret_cast<const char*>(&metadata_size), sizeof(int));

    out_file.close();
}

bool almost_equal(float a, float b, float epsilon = 1e-5f) {
    return std::fabs(a - b) < epsilon;
}

int main() {
    try {
        std::string hty_file_path = "test/test.hty";
        
        // Test extract_metadata
        json metadata = extract_metadata(hty_file_path);
        std::cout << "Metadata extracted successfully" << std::endl;
        
        // Print metadata for debugging
        std::cout << "Metadata contents:" << std::endl;
        std::cout << metadata.dump(2) << std::endl;
        
        // Test project_single_column
        std::string column_name = "id";
        std::vector<int> column_data = project_single_column(metadata, hty_file_path, column_name);
        std::cout << "Column '" << column_name << "' projected successfully" << std::endl;
        
        // Test display_column
        display_column(metadata, column_name, column_data);
        
        // Test filter
        std::vector<int> filtered_data = filter(metadata, hty_file_path, column_name, 4, 2); // Equal to 2
        std::cout << "Filtered data size: " << filtered_data.size() << std::endl;
        
        // Test project
        std::vector<std::string> projected_columns = {"id", "salary"};
        std::vector<std::vector<int>> projected_data = project(metadata, hty_file_path, projected_columns);
        std::cout << "Projected data size: " << projected_data.size() << " x " << projected_data[0].size() << std::endl;
        
        // Test display_result_set
        display_result_set(metadata, projected_columns, projected_data);
        
        // Test project_and_filter
        std::string filter_column = "salary";  // Make sure this column exists in your test.hty file
        std::cout << "Attempting to filter on column: " << filter_column << std::endl;
        
        // Check if the filter column exists in the metadata
        bool column_found = false;
        for (const auto& group : metadata["groups"]) {
            for (const auto& column : group["columns"]) {
                if (column["column_name"] == filter_column) {
                    column_found = true;
                    break;
                }
            }
            if (column_found) break;
        }
        
        if (!column_found) {
            std::cout << "Warning: Column '" << filter_column << "' not found in metadata. Available columns are:" << std::endl;
            for (const auto& group : metadata["groups"]) {
                for (const auto& column : group["columns"]) {
                    std::cout << "- " << column["column_name"] << std::endl;
                }
            }
        } else {
            std::vector<std::vector<int>> filtered_projected_data = project_and_filter(metadata, hty_file_path, projected_columns, filter_column, 2, 50000.0f); // salary less than 50000
            std::cout << "Filtered and projected data size: " << filtered_projected_data.size() << " x " << (filtered_projected_data.empty() ? 0 : filtered_projected_data[0].size()) << std::endl;
            
            // Display the filtered and projected data
            display_result_set(metadata, projected_columns, filtered_projected_data);
        }
        
        // Test add_row
        std::vector<std::vector<int>> new_rows = {{6, 2, 90000}, {7, 1, 32000}};
        std::string modified_hty_file_path = "modified_test.hty";
        add_row(metadata, hty_file_path, modified_hty_file_path, new_rows);
        std::cout << "Rows added successfully" << std::endl;
        
        // Verify the modified file
        json modified_metadata = extract_metadata(modified_hty_file_path);
        assert(modified_metadata["num_rows"].get<int>() == metadata["num_rows"].get<int>() + new_rows.size());
        std::cout << "Modified file verified successfully" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    std::cout << "All tests completed!" << std::endl;
    return 0;
}