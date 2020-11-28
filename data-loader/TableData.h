#ifndef TABLEDATA_H
#define TABLEDATA_H

#include <vector>
#include <string>

class TableData {
public:
    std::string sname;
    std::string rname;
    std::vector<std::string> anames;
    std::vector<std::vector<std::string>> datas;

    TableData(std::vector<std::string> anames);
    TableData(std::vector<std::string> anames, std::vector<std::vector<std::string>> datas);
};

#endif