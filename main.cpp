#include <iostream>
#include <fstream>
#include <string>
#include <vector>

#include "sql-processor/SQLProcessor.h"

void SplitString(const std::string& s, std::vector<std::string>& v, const std::string& c) {
    std::string::size_type pos1, pos2;
    pos2 = s.find(c);
    pos1 = 0;
    while(std::string::npos != pos2) {
        v.push_back(s.substr(pos1, pos2-pos1));
        pos1 = pos2 + c.size();
        pos2 = s.find(c, pos1);
    }
    if(pos1 != s.length())
        v.push_back(s.substr(pos1));
}

void solve_multi_query(std::string q) {
    std::vector<std::string> query_list;
    SplitString(q, query_list, ";");
    for(int i=0; i<query_list.size(); ++i) {
        std::string query = query_list[i];
        std::cout << "[" << i << "] " << query << std::endl;
        SQLProcessor p = SQLProcessor(query);
        // hsql::SQLParserResult result;
        // hsql::SQLParser::parseSQLString(query, &result);
        // if (result.isValid()) {
        //     hsql::SQLStatement* stat = result.getStatements()[0];
        //     SQLProcessor sql_processor = SQLProcessor(stat);
        //     sql_processor.solve();
        // } else {
        //     std::cout << query_error << " " << result.errorMsg() << std::endl;
        // }
    }
}

void solve_single_query(std::string query) {
    std::cout << query << std::endl;
    SQLProcessor p = SQLProcessor(query);
    // hsql::SQLParserResult result;
    // hsql::SQLParser::parseSQLString(query, &result);
    // if (result.isValid()) {
    //     hsql::SQLStatement* stat = result.getStatements()[0];
    //     SQLProcessor sql_processor = SQLProcessor(stat);
    //     sql_processor.solve();
    // } else {
    //     std::cout << query_error << result.errorMsg() << std::endl;
    // }
}

int main(int argc, char *argv[]) {
    // [TODO] start statement
    std::string logo = "  _____  _____  ____   _____ \n |  __ \\|  __ \\|  _ \\ / ____|\n | |  | | |  | | |_) | (___  \n | |  | | |  | |  _ < \\___ \\ \n | |__| | |__| | |_) |____) |\n |_____/|_____/|____/|_____/ \n";
    std::string start = "Welcome to the DDBS monitor.\nCommands end with `;`.\nType `help` for help.\nFor more information, visit: https://github.com/jaingmengmeng/DDBS\n";
    std::string system = "ddbs";
    std::string blank = "   -";
    std::string bye = "Bye";
    // [TODO] help statement
    std::string help = "Usage: 1) `./main`    2) `./main <filename>`";
    std::string file_error = "Error opening file. Please check your filename.";
    std::string query_error = "The SQL string is invalid!";

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
            if(query == "quit" || query == "exit") {
                std::cout << bye << std::endl;
                return 0;
            }
            if(query == "help") {
                std::cout << help << std::endl;
                // initial variables
                query = "";
                std::cout << system+"> ";
                continue;
            }
            if(str[str.size()-1] == ';') {
                // [TODO] process the query statements
                solve_single_query(query);
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
                    // [TODO] process the query statements
                    solve_single_query(query);
                    // initial variables
                    query = "";
                    // std::cout << system+"> ";
                } else {
                    // std::cout << blank+"> ";
                }
            }
        } else {
            std::cout << file_error << std::endl;
            exit(1);
        }
    } else {
        std::cout << help << std::endl;
    }
    return 0;
}