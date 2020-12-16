#include "Relation.h"

Relation::Relation(std::string rname, bool is_horizontal=true, int num_of_recs=0) :
rname(lower_string(rname)), is_horizontal(is_horizontal), num_of_recs(num_of_recs) {}

Relation::Relation(std::string rname, std::vector<Attribute> attributes, bool is_horizontal, int num_of_recs) :
rname(lower_string(rname)), attributes(attributes), is_horizontal(is_horizontal), num_of_recs(num_of_recs) {} 

std::ostream& operator<<(std::ostream& os, Relation r) {
    os << r.rname << std::string("(");
    // for(int i=0; i<r.attributes.size(); ++i) {
    //     if(i > 0)
    //         os << std::string(", ");
    //     os << r.attributes[i];
    // }
    os << combine_vector_string(r.get_attrs_meta());
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

void Relation::add_attribute(std::string aname, bool is_key, int type, int value_type, std::map<std::string, double> proportion) {
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

std::vector<std::string> Relation::get_anames() {
    std::vector<std::string> res;
    for(auto attribute : this->attributes) {
        res.push_back(attribute.aname);
    }
    return res;
}

std::vector<std::string> Relation::get_attrs_meta() {
    std::vector<std::string> res;
    for(auto attribute : this->attributes) {
        res.push_back(attribute.get_attr_meta());
    }
    return res;
}

std::vector<std::string> Relation::get_fragmented_attrs_meta(std::string sname) {
    if(this->is_horizontal) {
        return this->get_attrs_meta();
    } else {
        std::vector<std::string> aname_list;
        std::vector<std::string> res;
        for(auto fragment : this->frags) {
            if(fragment.sname == sname) {
                aname_list = fragment.vf_condition;
            }
        }
        for(auto aname : aname_list) {
            for(auto attribute : this->attributes) {
                if(attribute.aname == aname) {
                    res.push_back(attribute.get_attr_meta());
                }
            }
        }
        return res;
    }
}

bool Relation::in_site(std::string sname) {
    for(auto fragment : this->frags) {
        if(fragment.sname == sname) {
            return true;
        }
    }
    return false;
}
