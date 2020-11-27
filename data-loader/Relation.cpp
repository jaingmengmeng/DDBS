#include "Relation.h"

Relation::Relation(std::string rname) : rname(rname) {}

void Relation::add_attribute(Attribute a) {
    this->attributes.push_back(a);
}

void Relation::add_attribute(std::string aname, bool is_key, int type) {
    this->attributes.push_back(Attribute(aname, is_key, type));
}

void Relation::add_attribute(std::string aname, bool is_key, int type, int value_type, std::vector<double> value) {
    this->attributes.push_back(Attribute(aname, is_key, type, value_type, value));
}

void Relation::add_attribute(std::string aname, bool is_key, int type, int value_type, std::map<int, double> proportion) {
    this->attributes.push_back(Attribute(aname, is_key, type, value_type, proportion));
}

std::ostream& operator<<(std::ostream& os, Relation r) {
    os << r.rname << std::string("(");
    for(auto attribute : r.attributes) {
        os << attribute << std::string(",");
    }
    os << std::string(")");
    return os;
}