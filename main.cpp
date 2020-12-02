#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>

#include "sql-processor/SQLProcessor.h"
#include "data-loader/DataLoader.h"
#include "utils/utils.h"
#include "network-utils/network.h"

void solve_multi_query(std::string q, std::vector<Relation*> relations) {
    std::vector<std::string> query_list;
    SplitString(q, query_list, ";");
    for(int i=0; i<query_list.size(); ++i) {
        std::string query = query_list[i];
        // std::cout << "[" << i << "] " << query << std::endl;
        SQLProcessor processor = SQLProcessor(query, relations);
        if (processor.is_valid) {
            
        }
    }
}

void solve_single_query(std::string query, std::vector<Relation*> relations) {
    std::cout << query << std::endl;
    SQLProcessor processor = SQLProcessor(query, relations);
    if (processor.is_valid) {
        
    }
}

int main(int argc, char *argv[]) {
    // [TODO] start statement
    std::string logo =  "  _____  _____  ____   _____ \n"
                        " |  __ \\|  __ \\|  _ \\ / ____|\n"
                        " | |  | | |  | | |_) | (___  \n"
                        " | |  | | |  | |  _ < \\___ \\ \n"
                        " | |__| | |__| | |_) |____) |\n"
                        " |_____/|_____/|____/|_____/ \n";
    std::string start = "Welcome to the DDBS monitor.\n"
                        "Commands end with `;`.\n"
                        "Type `help` or `h` for help.\n"
                        "For more information, visit: https://github.com/jaingmengmeng/DDBS\n";
    std::string system = "ddbs";
    std::string blank = "   -";
    std::string bye = "Bye";
    std::string command_help =  "Command Usage:\t 1) `./main`\n"
                                "\t\t 2) `./main <filename>`";
    // [TODO] help statement
    std::string help =  "Usage:\t 1) `init`\n"
                        "\t 2) `show tables[;]`\n"
                        "\t 3) `load data[;]`\n"
                        "\t 4) `help` or `h`\n"
                        "\t 5) `select`\n"
                        "\t 6) `insert`\n"
                        "\t 7) `delete`\n"
                        "\t 8) `quit` or `q` or `exit`\n";
    std::string file_error = "Error opening file. Please check your filename.";
    std::string query_error = "The SQL string is invalid!";

    std::string query = "";
    std::string str = "";

    DataLoader data_loader = DataLoader();

    // read sql query from std input
    if(argc == 1) {
        std::cout << logo << std::endl << start << std::endl;
        std::cout << system+"> ";
        while(std::getline(std::cin, str)) {
            if(query != "")
                query += " ";
            query += str;
            // transform(query.begin(), query.end(), query.begin(), ::tolower);
            if(lower_string(query) == "quit" || lower_string(query) == "q" || lower_string(query) == "exit") {
                std::cout << bye << std::endl;
                return 0;
            }
            if(lower_string(query) == "init") {
                data_loader.init();
                // initial variables
                query = "";
                std::cout << system+"> ";
                continue;
            }
            if(lower_string(query) == "show tables" || lower_string(query) == "show tables;") {
                data_loader.show_tables();
                // initial variables
                query = "";
                std::cout << system+"> ";
                continue;
            }
            if(lower_string(query) == "load data" || lower_string(query) == "load data;") {
                // std::map<std::string, std::vector<std::string>> insert = data_loader.load_data();
                for(auto sname : data_loader.sites) {
                    for(auto relation : data_loader.relations) {
                        // Determine whether the relation table is assigned to the current site
                        if(relation->in_site(sname)) {
                            std::vector<std::string> insert_values = data_loader.import_data(relation->rname, sname);
                            std::string attr_meta = combine_vector_string(relation->get_fragmented_attrs_meta(sname));
                            // std::cout << attr_meta << std::endl;
                            int res = load_table(sname, relation->rname, attr_meta, insert_values);
                            std::cout << res << std::endl;
                        }
                    }
                }
                // initial variables
                query = "";
                std::cout << system+"> ";
                continue;
            }
            if(lower_string(query) == "help" || lower_string(query) == "h") {
                std::cout << help << std::endl;
                // initial variables
                query = "";
                std::cout << system+"> ";
                continue;
            }
            if(str[str.size()-1] == ';') {
                // process the query statements
                solve_single_query(query, data_loader.relations);
                // initial variables
                query = "";
                std::cout << system+"> ";
            } else {
                std::cout << blank+"> ";
            }
        }
    } else if(argc == 2) {
        std::ifstream fin(argv[1], std::ios_base::in);
        if (fin.is_open()) {
            while(std::getline(fin, str)) {
                if(query != "")
                    query += " ";
                query += str;
                if(str[str.size()-1] == ';') {
                    // process the query statements
                    solve_single_query(query, data_loader.relations);
                    // initial variables
                    query = "";
                }
            }
        } else {
            std::cout << file_error << std::endl;
            exit(1);
        }
    } else {
        std::cout << command_help << std::endl;
    }
    return 0;
}