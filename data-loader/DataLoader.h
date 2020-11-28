#ifndef DATALOADER_H
#define DATALOADER_H

#include <string>
#include <vector>
#include <iostream>
#include <map>
#include <fstream>

#include "Relation.h"

class DataLoader {
public:
    std::vector<Relation*> relations;
    std::map<std::string, std::string> files;
    std::vector<std::string> sites;

    DataLoader();
    void init();
    void show_tables();
    std::map<std::string, std::vector<std::string>> load_data();
    std::map<std::string, std::vector<std::vector<std::string>>> data_fragment(Relation* relation, std::vector<std::vector<std::string>> data);
};

#endif