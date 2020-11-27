#ifndef DATALOADER_H
#define DATALOADER_H

#include <string>
#include <vector>
#include <iostream>

#include "Relation.h"

class DataLoader {
public:
    std::vector<Relation*> relations;
    std::map<std::string, std::string> files;
    std::vector<std::string> sites;

    DataLoader();
    void init();
    void show_tables();
};

#endif