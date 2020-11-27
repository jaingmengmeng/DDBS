#ifndef SELECTSTATEMENT_H
#define SELECTSTATEMENT_H

#include <vector>
#include <string>

#include "../data-loader/Predicate.h"

class SelectStatement {
private:
    friend std::ostream& operator<<(std::ostream& os, SelectStatement s);
public:
    std::vector<std::string> from;
    std::vector<std::string> select;
    std::vector<Predicate> where;

    SelectStatement();
    void add_from(std::string table_name);
    void add_select(std::string attribute_name);
    void add_where(Predicate predicate);
};

#endif