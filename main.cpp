#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <map>
#include <set>
#include <boost/regex.hpp>

#include "sql-processor/SQLProcessor.h"
#include "data-loader/DataLoader.h"
#include "utils/utils.h"
#include "network-utils/network.h"
#include "sql-processor/SQLSelectProcessor.cpp"
#include "sql-processor/get_prefix.cpp"

int auto_increment_id = 0;

enum INPUT_TYPE {
    QUIT,
    INIT,
    SHOW_TABLES,
    SHOW_FRAGMENTS,
    SHOW_SITES,
    HELP,
    DEFINE_SITE,
    CREATE_TABLE,
    SQL_STATE,
};

void solve_multi_query(std::string q, std::vector<Relation*> relations) {
    std::vector<std::string> query_list;
    split_string(q, query_list, ";");
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
            std::string prefix = get_prefix(auto_increment_id++);
            get_query_tree(select_tree, rs, select_stat, prefix); //get result in select_tree
            std::map<std::string, std::string>::reverse_iterator iter;
            for(iter = select_tree.rbegin(); iter != select_tree.rend(); iter++){
                std::cout << iter->first << " " << iter->second << std::endl;
            }
            
            // [TODO]
            write_map_to_etcd(select_tree);
            std::set<std::string> temp_tables;
            for(auto iter : select_tree){
                temp_tables.insert(iter.first.substr(0, iter.first.find('.')));
            }
            std::string root_temp_table = *(temp_tables.cbegin());
            std::cout << "root: " << root_temp_table << std::endl;
            std::vector<std::string> rows = request_table(root_temp_table);
            for(const std::string& row : rows){
                std::cout << row << std::endl;
            }
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

INPUT_TYPE input_classifier(std::string input) {
    boost::regex re_quit("^((q(uit)?)|(e(xit)?))\\s*;?$", boost::regex::icase);
    boost::regex re_init("^init\\s*;?$", boost::regex::icase);
    boost::regex re_show_tables("^show\\s+tables\\s*;?$", boost::regex::icase);
    boost::regex re_show_fragments("^show\\s+fragments\\s*;?$", boost::regex::icase);
    boost::regex re_show_sites("^show\\s+sites\\s*;?$", boost::regex::icase);
    boost::regex re_help("^h(elp)?\\s*;?$", boost::regex::icase);
    boost::regex re_define_site("^define\\s+site\\s+[A-Za-z0-9]+\\s+[0-9.]+:[0-9]+(\\s*,\\s*[A-Za-z0-9]+\\s+[0-9.]+:[0-9]+)*\\s*;?$", boost::regex::icase);
    boost::regex re_create_table("^create\\s+table\\s+[A-Za-z0-9]+\\s*\\(\\s*[A-Za-z]+\\s+(int|char\\s*\\(\\s*[0-9]+\\s*\\))(\\s+key)?(\\s*,\\s*[A-Za-z]+\\s+(int|char\\s*\\(\\s*[0-9]+\\s*\\))(\\s+key)?\\s*)*\\s*\\)\\s*;?$", boost::regex::icase);
    if(boost::regex_match(input, re_quit)) {
        return QUIT;
    } else if(boost::regex_match(input, re_init)) {
        return INIT;
    } else if(boost::regex_match(input, re_show_tables)) {
        return SHOW_TABLES;
    } else if(boost::regex_match(input, re_show_fragments)) {
        return SHOW_FRAGMENTS;
    } else if(boost::regex_match(input, re_show_sites)) {
        return SHOW_SITES;
    } else if(boost::regex_match(input, re_help)) {
        return HELP;
    } else if(boost::regex_match(input, re_define_site)) {
        return DEFINE_SITE;
    } else if(boost::regex_match(input, re_create_table)) {
        return CREATE_TABLE;
    } else {
        return SQL_STATE;
    }
}

int main(int argc, char *argv[]) {
    // [TODO] start statement
    const std::string logo =    "  _____  _____  ____   _____ \n"
                                " |  __ \\|  __ \\|  _ \\ / ____|\n"
                                " | |  | | |  | | |_) | (___  \n"
                                " | |  | | |  | |  _ < \\___ \\ \n"
                                " | |__| | |__| | |_) |____) |\n"
                                " |_____/|_____/|____/|_____/ \n";
    const std::string start =   "Welcome to the DDBS monitor.\n"
                                "Commands end with `;`.\n"
                                "Type `help` or `h` for help.\n"
                                "For more information, visit: https://github.com/jaingmengmeng/DDBS\n";
    const std::string system = "ddbs";
    const std::string blank = "   -";
    const std::string bye = "Bye";
    const std::string command_help =  "Command Usage:\t 1) `./main`\n"
                                "\t\t 2) `./main <filename>`";
    // [TODO] help statement
    const std::string help =    "Usage:\t 1) `init`\n"
                                "\t 2) `show tables[;]`\n"
                                "\t 3) `load data[;]`\n"
                                "\t 4) `help` or `h`\n"
                                "\t 5) `select`\n"
                                "\t 6) `insert`\n"
                                "\t 7) `delete`\n"
                                "\t 8) `quit` or `q` or `exit`\n";
    const std::string file_error = "Error opening file. Please check your filename.";
    const std::string query_error = "The SQL string is invalid!";

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
            query = trim(query);
            INPUT_TYPE input_type = input_classifier(query);
            if(input_type == DEFINE_SITE) {
                std::vector<std::string> v_sites;
                boost::regex tmp_define_site("(define\\s+site\\s+)([^;]+)(;?)", boost::regex::icase);
                // delete the `define site` string
                query = boost::regex_replace(query, tmp_define_site, "$2");
                split_string(query, v_sites, ",");
                for(auto site : v_sites) {
                    // get sname, ip, port
                    site = trim(site);
                    std::string sname = trim(site.substr(0, site.find_first_of(" ")-0));
                    std::string ip =  trim(site.substr(site.find_first_of(" ")+1, site.find_first_of(":")-site.find_first_of(" ")-1));
                    std::string port =  trim(site.substr(site.find(":")+1));
                    data_loader.add_site(sname, ip, port);
                }
                // initial variables
                query = "";
                std::cout << system+"> ";
            } else if(input_type == CREATE_TABLE) {
                // delete the `create table` string
                boost::regex tmp_create_table("(create\\s+table\\s+)([^;]+)(;?)", boost::regex::icase);
                query = trim(boost::regex_replace(query, tmp_create_table, "$2"));
                // get rname
                std::string rname = query.substr(0, query.find_first_of(" ")-0);
                // get attributes
                std::vector<Attribute> attributes;
                std::vector<std::string> v_attributes;
                std::string attributes_str = query.substr(query.find_first_of("(")+1, query.find_last_of(")")-query.find_first_of("(")-1);
                std::cout << attributes_str << std::endl;
                split_string(attributes_str, v_attributes, ",");
                for(auto attribute : v_attributes) {
                    attribute = trim(attribute);
                    std::cout << attribute << std::endl;
                    std::vector<std::string> v_str;
                    split_string(attribute, v_str, " ");
                    for(auto& str : v_str) {
                        str = trim(str);
                        std::cout << "#" << str << "#" << std::endl;
                    }
                    bool is_key = v_str.size() == 3 && v_str[2] == "key";
                    int type = (v_str[1] == "int") ? 1 : 2;
                    std::cout << v_str[0] << " " << is_key << " " << type << "#" << std::endl;
                    attributes.push_back(Attribute(v_str[0], is_key, type));
                }
                data_loader.add_relation(rname, attributes);
                // initial variables
                query = "";
                std::cout << system+"> ";
            } else if(input_type == INIT) {
                data_loader.init();
                // initial variables
                query = "";
                std::cout << system+"> ";
            } else if(input_type == SHOW_TABLES) {
                data_loader.show_tables();
                // initial variables
                query = "";
                std::cout << system+"> ";
            } else if(input_type == SHOW_FRAGMENTS) {
                // data_loader.show_fragments();
                // initial variables
                query = "";
                std::cout << system+"> ";
            } else if(input_type == SHOW_SITES) {
                data_loader.show_sites();
                // initial variables
                query = "";
                std::cout << system+"> ";
            } else if(input_type == HELP) {
                std::cout << help << std::endl;
                // initial variables
                query = "";
                std::cout << system+"> ";
            } else if(input_type == QUIT) {
                std::cout << bye << std::endl;
                return 0;
            } else {
                // insert; delete; select;
                if(query[query.size()-1] == ';') {
                    // process the query statements
                    solve_single_query(query, data_loader.relations);
                    // initial variables
                    query = "";
                    std::cout << system+"> ";
                } else {
                    std::cout << blank+"> ";
                }
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
