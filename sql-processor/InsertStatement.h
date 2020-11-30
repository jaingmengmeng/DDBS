#ifndef INSERTSTATEMENT_H
#define INSERTSTATEMENT_H

#include <vector>
#include <string>

#include "../data-loader/Predicate.h"

class InsertStatement {
private:
    friend std::ostream& operator<<(std::ostream& os, InsertStatement s);
public:
    std::vector<std::string> from;
    std::vector<std::string> select;
    std::vector<Predicate> where;
};

#endif