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
DataLoader data_loader = DataLoader();

enum INPUT_TYPE {
    QUIT,
    INIT,
    SHOW_TABLES,
    SHOW_FRAGMENTS,
    SHOW_SITES,
    HELP,
    DEFINE_SITE,
    CREATE_TABLE,
    FRAGMENT,
    ALLOCATE,
    SQL_STATE,
};

std::string execute_insert_sql(const std::string& sname, const std::string& rname, const std::string& values) {
    Site s = data_loader.get_site_by_sname(sname);
    std::string ip = s.get_url();
    std::string insert_sql = "INSERT INTO " + rname + " VALUES ("+ values +");";
    std::cout << ip << " " << insert_sql << std::endl;
    return execute_non_query_sql(ip, insert_sql);
}

std::string execute_delete_sql(const std::string& sname, const std::string& rname, const std::vector<Predicate>& where) {
    Site s = data_loader.get_site_by_sname(sname);
    std::string ip = s.get_url();
    std::string delete_sql = "DELETE FROM " + rname + " WHERE ";
    for(int i=0; i<where.size(); ++i) {
        if(i > 0)
            delete_sql += " AND ";
        delete_sql += where[i].to_string();
    }
    std::cout << ip << " " << delete_sql << std::endl;
    return execute_non_query_sql(ip, delete_sql);
}

void solve_multi_query(std::string q) {
    std::vector<std::string> query_list;
    split_string(q, query_list, ";");
    for(int i=0; i<query_list.size(); ++i) {
        std::string query = query_list[i];
        // std::cout << "[" << i << "] " << query << std::endl;
        SQLProcessor processor = SQLProcessor(query, data_loader.relations);
        if (processor.is_valid) {
                   
        }
    }
}

void solve_single_query(std::string query) {
    std::cout << query << std::endl;
    SQLProcessor processor = SQLProcessor(query, data_loader.relations);
    if (processor.is_valid) {
        // select
        if(processor.sql_type == 1) {
            SelectStatement select_stat = processor.select;
            std::cout << select_stat << std::endl;
            std::map<std::string, std::string> select_tree;
            std::string prefix = get_prefix(auto_increment_id++);
            get_query_tree(select_tree, data_loader.relations, select_stat, prefix); //get result in select_tree
            std::map<std::string, std::string>::reverse_iterator iter;
            for(iter = select_tree.rbegin(); iter != select_tree.rend(); iter++){
                std::cout << iter->first << " " << iter->second << std::endl;
            }
            
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
            InsertStatement insert_stat = processor.insert;
            std::cout << insert_stat << std::endl;
            // [TODO] 
            std::unordered_map<std::string, std::string> insert_map = data_loader.get_site_to_insert(insert_stat.rname, insert_stat.values);
            std::unordered_map<std::string, std::string>::const_iterator iter;
            for(iter = insert_map.begin(); iter != insert_map.end(); iter++) {
                std::cout << iter->first << " " << iter->second << std::endl;
                std::string result = execute_insert_sql(iter->first, insert_stat.rname, iter->second);
                std::cout << result << std::endl;
            }
        }
        // delete
        else if(processor.sql_type == 3) {
            DeleteStatement delete_stat = processor.delete_s;
            std::cout << delete_stat << std::endl;
            // [TODO]
            std::unordered_map<std::string, std::vector<Predicate>> delete_map = data_loader.get_site_to_delete(delete_stat.rname, delete_stat.where);
            std::unordered_map<std::string, std::vector<Predicate>>::const_iterator iter;
            for(iter = delete_map.begin(); iter != delete_map.end(); iter++) {
                std::cout << iter->first << std::endl;
                std::string result = execute_delete_sql(iter->first, delete_stat.rname, iter->second);
                std::cout << result << std::endl;
            }
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
    boost::regex re_create_table("^create\\s+table\\s+[A-Za-z0-9]+\\s*\\(\\s*[A-Za-z_]+\\s+(int|char\\s*\\(\\s*[0-9]+\\s*\\))(\\s+key)?(\\s*,\\s*[A-Za-z]+\\s+(int|char\\s*\\(\\s*[0-9]+\\s*\\))(\\s+key)?\\s*)*\\s*\\)\\s*;?$", boost::regex::icase);
    boost::regex re_fragment("^fragment\\s+[A-Za-z0-9]+\\s+(horizontally|vertically)\\s+into\\s+[^;]+\\s*;?$", boost::regex::icase);
    boost::regex re_allocate("^allocate\\s+[A-Za-z0-9\\.]+\\s+to\\s+[A-Za-z0-9]+\\s*;?$", boost::regex::icase);
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
    } else if(boost::regex_match(input, re_fragment)) {
        return FRAGMENT;
    } else if(boost::regex_match(input, re_allocate)) {
        return ALLOCATE;
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
                query = lower_string(query);
                // delete the `define site` string
                boost::regex tmp_define_site("(define\\s+site\\s+)([^;]+)(;?)");
                query = boost::regex_replace(query, tmp_define_site, "$2");
                std::vector<std::string> v_sites;
                split_string(query, v_sites, ",");
                for(auto site : v_sites) {
                    // get sname, ip, port
                    site = trim(site);
                    std::string sname = lower_string(trim(site.substr(0, site.find_first_of(" ")-0)));
                    std::string ip =  trim(site.substr(site.find_first_of(" ")+1, site.find_first_of(":")-site.find_first_of(" ")-1));
                    std::string port =  trim(site.substr(site.find(":")+1));
                    // add site
                    data_loader.add_site(sname, ip, port);
                }
                // initial variables
                query = "";
                std::cout << system+"> ";
            } else if(input_type == CREATE_TABLE) {
                query = lower_string(query);
                // delete the `create table` string
                boost::regex tmp_create_table("(create\\s+table\\s+)([^;]+)(;?)");
                query = trim(boost::regex_replace(query, tmp_create_table, "$2"));
                // get rname
                std::string rname = trim(query.substr(0, query.find_first_of("(")));
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
                // add relation
                data_loader.add_relation(rname, attributes);
                // initial variables
                query = "";
                std::cout << system+"> ";
            } else if(input_type == FRAGMENT) {
                // get is_horizontal & fragment conditions
                boost::regex tmp_fragment("(fragment\\s+)([A-Za-z0-9]+\\s+)(horizontally|vertically)(\\s+into\\s+)([^;]+)(\\s*;?)");
                std::string rname = lower_string(trim(boost::regex_replace(query, tmp_fragment, "$2")));
                bool is_horizontal = (trim(boost::regex_replace(query, tmp_fragment, "$3")) == "horizontally") ? true : false;
                std::string conditions = trim(boost::regex_replace(query, tmp_fragment, "$5"));
                // add fragments
                if(is_horizontal) {
                    std::vector<std::string> v_condition;
                    split_string(conditions, v_condition, ",");
                    for(int i=0; i<v_condition.size(); ++i) {
                        std::string fname = rname+"."+std::to_string(i+1);
                        std::vector<Predicate> hf_condition;
                        std::vector<std::string> v_predicate;
                        split_string(v_condition[i], v_predicate, "and");
                        // build a fragment instance and add fragment
                        for(int j=0; j<v_predicate.size(); ++j) {
                            boost::regex tmp_predicate("([A-Za-z_]+\\s*)(>|>=|<|<=|=|<>)(\\s*[\"'A-Za-z0-9]+)");
                            std::string aname = trim(boost::regex_replace(v_predicate[j], tmp_predicate, "$1"));
                            std::string op = trim(boost::regex_replace(v_predicate[j], tmp_predicate, "$2"));
                            std::string value = trim(boost::regex_replace(v_predicate[j], tmp_predicate, "$3"));
                            std::cout << aname << " " << op << " " << value << "#" << std::endl;
                            int op_type;
                            if(value[0] == '\'' || value[0] == '"') {
                                if(op == "=") op_type = 6;
                                else if(op == "<>") op_type = 9;
                                hf_condition.push_back(Predicate(op_type, aname, value.substr(1, value.size()-2)));
                            } else {
                                if(op == ">=") op_type = 1;
                                else if(op == "<=") op_type = 2;
                                else if(op == ">") op_type = 3;
                                else if(op == "<") op_type = 4;
                                else if(op == "=") op_type = 5;
                                else if(op == "<>") op_type = 8;
                                hf_condition.push_back(Predicate(op_type, aname, std::stod(value)));
                            }
                        }
                        // [TODO] to vertify the fragment statement is valid --- the fragment is complete & nonexistent.
                        data_loader.add_unallocated_fragment(Fragment(rname, fname, is_horizontal, hf_condition));
                    }
                } else {
                    std::vector<std::string> v_condition;
                    while(conditions.find_first_of("(") != std::string::npos) {
                        std::string tem_condition = conditions.substr(conditions.find_first_of("(")+1, conditions.find_first_of(")")-conditions.find_first_of("(")-1);
                        v_condition.push_back(tem_condition);
                        conditions = conditions.substr(conditions.find_first_of(")")+1);
                    }
                    for(int i=0; i<v_condition.size(); ++i) {
                        std::string fname = rname+"."+std::to_string(i+1);
                        std::vector<std::string> v_aname;
                        v_condition[i] = trim(v_condition[i].substr(v_condition[i].find_first_of("(")+1, v_condition[i].find_first_of(")")-v_condition[i].find_first_of("(")-1));
                        split_string(v_condition[i], v_aname, ",");
                        std::vector<std::string> vf_condition;
                        for(int j=0; j<v_aname.size(); ++j) {
                            std::string aname = trim(v_aname[j]);
                            vf_condition.push_back(aname);
                        }
                        data_loader.add_unallocated_fragment(Fragment(rname, fname, is_horizontal, vf_condition));
                    }
                }
                // initial variables
                query = "";
                std::cout << system+"> ";
            } else if(input_type == ALLOCATE) {
                query = lower_string(query);
                // get fname & sname
                boost::regex tmp_allocate("(allocate\\s+)([A-Za-z0-9\\.]+)(\\s+to\\s+)([A-Za-z0-9]+)(\\s*;?)");
                std::string fname = trim(boost::regex_replace(query, tmp_allocate, "$2"));
                std::string sname = trim(boost::regex_replace(query, tmp_allocate, "$4"));
                std::cout << fname << " " << sname << "#" << std::endl;
                // allocate the fragment to the site
                data_loader.allocate(fname, sname);
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
                data_loader.show_unallocated_fragments();
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
                    solve_single_query(query);
                    // initial variables
                    query = "";
                    std::cout << system+"> ";
                } else {
                    std::cout << blank+"> ";
                }
            }
        }
    } else if(argc == 2) {
        std::cout << logo << std::endl << start << std::endl;
        std::cout << system+"> ";
        std::ifstream fin(argv[1], std::ios_base::in);
        if (fin.is_open()) {
            while(std::getline(fin, str)) {
                if(query != "")
                    query += " ";
                query += str;
                query = trim(query);
                std::cout << query << std::endl;
                INPUT_TYPE input_type = input_classifier(query);
                if(input_type == DEFINE_SITE) {
                    query = lower_string(query);
                    // delete the `define site` string
                    boost::regex tmp_define_site("(define\\s+site\\s+)([^;]+)(;?)");
                    query = boost::regex_replace(query, tmp_define_site, "$2");
                    std::vector<std::string> v_sites;
                    split_string(query, v_sites, ",");
                    for(auto site : v_sites) {
                        // get sname, ip, port
                        site = trim(site);
                        std::string sname = lower_string(trim(site.substr(0, site.find_first_of(" ")-0)));
                        std::string ip =  trim(site.substr(site.find_first_of(" ")+1, site.find_first_of(":")-site.find_first_of(" ")-1));
                        std::string port =  trim(site.substr(site.find(":")+1));
                        // add site
                        data_loader.add_site(sname, ip, port);
                    }
                    // initial variables
                    query = "";
                    std::cout << system+"> ";
                } else if(input_type == CREATE_TABLE) {
                    query = lower_string(query);
                    // delete the `create table` string
                    boost::regex tmp_create_table("(create\\s+table\\s+)([^;]+)(;?)");
                    query = trim(boost::regex_replace(query, tmp_create_table, "$2"));
                    // get rname
                    std::string rname = trim(query.substr(0, query.find_first_of("(")));
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
                    // add relation
                    data_loader.add_relation(rname, attributes);
                    // initial variables
                    query = "";
                    std::cout << system+"> ";
                } else if(input_type == FRAGMENT) {
                    // get is_horizontal & fragment conditions
                    boost::regex tmp_fragment("(fragment\\s+)([A-Za-z0-9]+\\s+)(horizontally|vertically)(\\s+into\\s+)([^;]+)(\\s*;?)");
                    std::string rname = lower_string(trim(boost::regex_replace(query, tmp_fragment, "$2")));
                    bool is_horizontal = (trim(boost::regex_replace(query, tmp_fragment, "$3")) == "horizontally") ? true : false;
                    std::string conditions = trim(boost::regex_replace(query, tmp_fragment, "$5"));
                    // add fragments
                    if(is_horizontal) {
                        std::vector<std::string> v_condition;
                        split_string(conditions, v_condition, ",");
                        for(int i=0; i<v_condition.size(); ++i) {
                            std::string fname = rname+"."+std::to_string(i+1);
                            std::vector<Predicate> hf_condition;
                            std::vector<std::string> v_predicate;
                            split_string(v_condition[i], v_predicate, "and");
                            // build a fragment instance and add fragment
                            for(int j=0; j<v_predicate.size(); ++j) {
                                boost::regex tmp_predicate("([A-Za-z_]+\\s*)(>|>=|<|<=|=|<>)(\\s*[\"'A-Za-z0-9]+)");
                                std::string aname = trim(boost::regex_replace(v_predicate[j], tmp_predicate, "$1"));
                                std::string op = trim(boost::regex_replace(v_predicate[j], tmp_predicate, "$2"));
                                std::string value = trim(boost::regex_replace(v_predicate[j], tmp_predicate, "$3"));
                                std::cout << aname << " " << op << " " << value << "#" << std::endl;
                                int op_type;
                                if(value[0] == '\'' || value[0] == '"') {
                                    if(op == "=") op_type = 6;
                                    else if(op == "<>") op_type = 9;
                                    hf_condition.push_back(Predicate(op_type, aname, value.substr(1, value.size()-2)));
                                } else {
                                    if(op == ">=") op_type = 1;
                                    else if(op == "<=") op_type = 2;
                                    else if(op == ">") op_type = 3;
                                    else if(op == "<") op_type = 4;
                                    else if(op == "=") op_type = 5;
                                    else if(op == "<>") op_type = 8;
                                    hf_condition.push_back(Predicate(op_type, aname, std::stod(value)));
                                }
                            }
                            // [TODO] to vertify the fragment statement is valid --- the fragment is complete & nonexistent.
                            data_loader.add_unallocated_fragment(Fragment(rname, fname, is_horizontal, hf_condition));
                        }
                    } else {
                        std::vector<std::string> v_condition;
                        while(conditions.find_first_of("(") != std::string::npos) {
                            std::string tem_condition = conditions.substr(conditions.find_first_of("(")+1, conditions.find_first_of(")")-conditions.find_first_of("(")-1);
                            v_condition.push_back(tem_condition);
                            conditions = conditions.substr(conditions.find_first_of(")")+1);
                        }
                        for(int i=0; i<v_condition.size(); ++i) {
                            std::string fname = rname+"."+std::to_string(i+1);
                            std::vector<std::string> v_aname;
                            v_condition[i] = trim(v_condition[i].substr(v_condition[i].find_first_of("(")+1, v_condition[i].find_first_of(")")-v_condition[i].find_first_of("(")-1));
                            split_string(v_condition[i], v_aname, ",");
                            std::vector<std::string> vf_condition;
                            for(int j=0; j<v_aname.size(); ++j) {
                                std::string aname = trim(v_aname[j]);
                                vf_condition.push_back(aname);
                            }
                            data_loader.add_unallocated_fragment(Fragment(rname, fname, is_horizontal, vf_condition));
                        }
                    }
                    // initial variables
                    query = "";
                    std::cout << system+"> ";
                } else if(input_type == ALLOCATE) {
                    query = lower_string(query);
                    // get fname & sname
                    boost::regex tmp_allocate("(allocate\\s+)([A-Za-z0-9\\.]+)(\\s+to\\s+)([A-Za-z0-9]+)(\\s*;?)");
                    std::string fname = trim(boost::regex_replace(query, tmp_allocate, "$2"));
                    std::string sname = trim(boost::regex_replace(query, tmp_allocate, "$4"));
                    std::cout << fname << " " << sname << "#" << std::endl;
                    // allocate the fragment to the site
                    data_loader.allocate(fname, sname);
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
                    data_loader.show_unallocated_fragments();
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
                        solve_single_query(query);
                        // initial variables
                        query = "";
                        std::cout << system+"> ";
                    } else {
                        std::cout << blank+"> ";
                    }
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
