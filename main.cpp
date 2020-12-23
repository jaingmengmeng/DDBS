#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <map>
#include <set>

#include "sql-processor/SQLProcessor.h"
#include "data-loader/DataLoader.h"
#include "utils/utils.h"
#include "network-utils/network.h"
#include "sql-processor/SQLSelectProcessor.cpp"
#include "sql-processor/get_prefix.cpp"

int auto_increment_id = 0;


void print_node(std::map<std::string, std::string>& tree_mp, std::string node, std::string site, int indent){
    if (indent != 0)
    {
        std::cout << "|"; 
    }
    for (int i = 0; i < indent; i++)
    {
        std::cout << "-";
    }

    std::string node_str = node + "(";
    node_str += site + ", ";
    std::string key = node + ".project";
    if (tree_mp.find(key) != tree_mp.end())
    {
        node_str += "project = " + tree_mp[key] + ", ";
    }
    key = node + ".select";
    if (tree_mp.find(key) != tree_mp.end())
    {
        node_str += "select = " + tree_mp[key] + ", ";
    }
    key = node + ".combine";
    if (tree_mp.find(key) != tree_mp.end())
    {
        node_str += "combine = " + tree_mp[key] + ", ";
    }
    node_str += ")";
    std::cout << node_str << "\n\n";
}

void print_query_tree(std::map<std::string, std::string>& tree_mp, std::map<string, string> node2site, std::string parent_node, int indent){

    print_node(tree_mp, parent_node, node2site[parent_node], indent);
  
    std::string children = tree_mp[parent_node + ".children"];
    std::string type = tree_mp[parent_node + ".type"];
    if (type == "L")
    {
        if (indent + 4 != 0)
        {
            std::cout << "|"; 
        }
        for (int i = 0; i < indent + 4; i++)
        {
            std::cout << "-";
        }
        std::cout << children << "\n\n";
    }
    else
    {
        std::string temp;
        int index1 = 0, index2 = 0;
        while (index2 != std::string::npos)
        {
            index2 = children.find("|", index1);
            // if (index2 != std::string::npos)
            // {
                temp = children.substr(index1, index2 - index1);
                int index3 = temp.find(":");
                std::string node = temp.substr(0, index3);
                print_query_tree(tree_mp, node2site, node, indent + 4);
                index1 = index2 + 1;
            // }
        }   
    }
}


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
        // select
        if(processor.sql_type == 1) {
            SelectStatement select_stat = processor.select;
            std::map<std::string, std::string> select_tree;

            std::vector<Relation> rs;
            for(int i=0;i<relations.size();i++){
                rs.push_back(*relations[i]);
            }
            // std::cout << "111";
            std::string prefix = get_prefix(auto_increment_id++);
            std::map<std::string, std::string> node2site;
            get_query_tree(select_tree, node2site, rs, select_stat, prefix); //get result in select_tree
            std::map<std::string, std::string>::reverse_iterator iter;
            // for(iter = select_tree.rbegin(); iter != select_tree.rend(); iter++){
            //     std::cout << iter->first << " " << iter->second << std::endl;
            // }
            

            std::set<std::string> temp_tables;
            for(auto iter : select_tree){
                temp_tables.insert(iter.first.substr(0, iter.first.find('.')));
            }
            std::string root_temp_table = *(temp_tables.cbegin());
            node2site[root_temp_table] = "site" + std::to_string(get_site_no());
            // std::cout << "root: " << root_temp_table << std::endl;
            print_query_tree(select_tree, node2site, root_temp_table, 0);

            // [TODO]
            write_map_to_etcd(select_tree);
            std::vector<std::string> rows = request_table(root_temp_table);
            // for(const std::string& row : rows){
            //     std::cout << row << std::endl;
            // }
            std::cout << "total: " << rows.size() << " rows" << std::endl;
            std::vector<std::string> v(temp_tables.cbegin(), temp_tables.cend());
            try
            {
                std::map<std::string, std::string> statistics = get_request_statistics(v);
                // latency and cc
                std::string lm = statistics[root_temp_table];
                // std::cout << "root temp table lm: " << lm << std::endl;
                long latency = std::stol(lm.substr(0, lm.find(',')));
                long cc = 0;
                for(auto iter : statistics){
                    // std::cout << iter.first << ": " << iter.second << std::endl;
                    cc += std::stol(iter.second.substr(iter.second.find(',') + 1));
                }
                std::cout << "latency(MS): " << latency / 1000.0 << std::endl;
                std::cout << "communication cost(KB): " << cc / 1000.0 << std::endl;
            }
            catch(const std::exception& e)
            {
                std::cerr << e.what() << '\n';
            }

            // delete query tree in etcd
            delete_from_etcd_by_prefix(root_temp_table);
            
        }
        // insert
        else if(processor.sql_type == 2) {
            InsertStatement select_stat = processor.insert;
        }
        // delete
        else if(processor.sql_type == 3) {
            DeleteStatement select_stat = processor.delete_s;
        }
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
            // load all fragmented data to all sites
            if(lower_string(query) == "load data" || lower_string(query) == "load data;") {
                for(auto site : data_loader.sites) {
                    for(auto relation : data_loader.relations) {
                        // Determine whether the relation table is assigned to the current site
                        if(relation->in_site(site.sname)) {
                            std::vector<std::string> insert_values = data_loader.import_fragmented_data(site.sname, relation->rname);
                            std::string attr_meta = combine_vector_string(relation->get_fragmented_attrs_meta(site.sname));
                            int res = load_table(site.get_url(), site.sname+std::string("_")+relation->rname, attr_meta, insert_values);
                            // std::cout << res << std::endl;
                        }
                    }
                }
                // initial variables
                query = "";
                std::cout << system+"> ";
                continue;
            }
            // load all fragmented data to one site
            if(lower_string(query) == "load all frag data" || lower_string(query) == "load all frag data;") {
                for(auto site : data_loader.sites) {
                    for(auto relation : data_loader.relations) {
                        // Determine whether the relation table is assigned to the current site
                        if(relation->in_site(site.sname)) {
                            std::vector<std::string> insert_values = data_loader.import_fragmented_data(site.sname, relation->rname);
                            std::string attr_meta = combine_vector_string(relation->get_fragmented_attrs_meta(site.sname));
                            int res = load_table("127.0.0.1:8000", site.sname+std::string("_")+relation->rname, attr_meta, insert_values);
                            // std::cout << res << std::endl;
                        }
                    }
                }
                // initial variables
                query = "";
                std::cout << system+"> ";
                continue;
            }
            // load all global data to one site
            if(lower_string(query) == "load all global data" || lower_string(query) == "load all global data;") {
                for(auto relation : data_loader.relations) {
                    std::vector<std::string> insert_values = data_loader.import_data(relation->rname);
                    std::string attr_meta = combine_vector_string(relation->get_attrs_meta());
                    int res = load_table("127.0.0.1:8000", relation->rname, attr_meta, insert_values);
                    // std::cout << res << std::endl;
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
