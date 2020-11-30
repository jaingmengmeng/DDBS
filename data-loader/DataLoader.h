#ifndef DATALOADER_H
#define DATALOADER_H

#include <string>
#include <vector>
#include <iostream>
#include <map>
#include <fstream>

#include "Relation.h"
#include "../utils/utils.h"

class DataLoader {
private:
    std::map<std::string, std::vector<std::vector<std::string>>> data_fragment(Relation* relation);
    Relation* get_relation(std::string rname);
public:
    std::vector<Relation*> relations;
    std::map<std::string, std::string> files;
    std::vector<std::string> sites;
    std::map<std::string, std::vector<std::vector<std::string>>> datas; // relations
    std::map<std::string, std::map<std::string, std::vector<std::vector<std::string>>>> fragmented_datas;   // relations, sites

    DataLoader();
    void init();    // initial the relations, sites variables.
    void show_tables(bool show_fragment=true);   // show all tables & fragments information.
    void load_data();   // initial (data/fragmented data) from files.
    std::vector<std::string> import_data(std::string sname, std::string rname);   // load data to servers
    std::string import_data_sql(std::string sname, std::string rname);    // load data to servers via sql
};

#endif