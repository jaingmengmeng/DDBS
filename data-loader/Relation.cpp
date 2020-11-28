#include "Relation.h"

Relation::Relation(std::string rname, bool is_horizontal=true) :
rname(rname), is_horizontal(is_horizontal) {
    this->num_of_recs = 0;
}

std::ostream& operator<<(std::ostream& os, Relation r) {
    os << r.rname << std::string("(");
    for(int i=0; i<r.attributes.size(); ++i) {
        if(i > 0)
            os << std::string(", ");
        os << r.attributes[i];
    }
    os << std::string(") ");
    if(r.num_of_recs > 0) {
        os << r.num_of_recs;
    }
    os << std::string("\n");
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

void Relation::set_num_of_recs(int n) {
    this->num_of_recs = n;
}
