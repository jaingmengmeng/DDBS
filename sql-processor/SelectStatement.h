#ifndef SELECTSTATEMENT_H
#define SELECTSTATEMENT_H

#include <vector>
#include <string>

#include "Predicate.h"

class SelectStatement {
public:
    std::vector<std::string> from;
    std::vector<std::string> select;
    std::vector<Predicate> where;

    SelectStatement() {
    }
};
#endif