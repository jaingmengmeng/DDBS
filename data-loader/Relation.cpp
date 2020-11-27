#include "Relation.h"

Relation::Relation(std::string rname) : rname(rname) {}

std::ostream& operator<<(std::ostream& os, Relation r) {
    os << r.rname << std::string("(");
    // for(auto attribute : r.attributes) {
    //     os << attribute << std::string(",");
    // }
    for(int i=0; i<r.attributes.size(); ++i) {
        if(i > 0)
            os << std::string(", ");
        os << r.attributes[i];
    }
    os << std::string(")\n");
    return os;
}

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

void Relation::add_fragment(Fragment f) {
    this->frags.push_back(f);
}

void Relation::add_fragment(std::string rname, std::string fname, std::string sname, bool is_horizontal, std::vector<std::string> vf_condition) {
    this->frags.push_back(Fragment(rname, fname, sname, is_horizontal, vf_condition));
}

void Relation::add_fragment(std::string rname, std::string fname, std::string sname, bool is_horizontal, std::vector<Predicate> hf_condition) {
    this->frags.push_back(Fragment(rname, fname, sname, is_horizontal, hf_condition));
}

void Relation::print_fragments() {
    for(auto fragment : this->frags) {
        std::cout << fragment << std::endl;
    }
}
