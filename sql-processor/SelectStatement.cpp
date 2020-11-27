#include <vector>
#include <string>
#include <iostream>

#include "SelectStatement.h"

SelectStatement::SelectStatement() {}

void SelectStatement::add_from(std::string table_name) {
    this->from.push_back(table_name);
}

void SelectStatement::add_select(std::string attribute_name) {
    this->select.push_back(attribute_name);
}

void SelectStatement::add_where(Predicate predicate) {
    this->where.push_back(predicate);
}

std::ostream& operator<<(std::ostream& os, SelectStatement s) {
    os << "from_list: ";
    for(int i=0; i<s.from.size(); ++i) {
        os << s.from[i] << "\t";
    }
    os << "\n";
    os << "select_list: ";
    for(auto attribute : s.select) {
        os << attribute << "\t";
    }
    os << "\n";
    return os;
}