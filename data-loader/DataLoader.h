#ifndef DATALOADER_H
#define DATALOADER_H

#include <string>
#include <vector>
#include <iostream>

#include "Relation.h"

class DataLoader {
public:
    std::vector<Relation*> relations;

    DataLoader();
    void init();
};

#endif