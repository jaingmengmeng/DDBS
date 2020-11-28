#ifndef RELATION_H
#define RELATION_H

#include <vector>
#include <string>
#include <iostream>

#include "Fragment.h"
#include "Attribute.h"

class Relation {
private:
    friend std::ostream& operator<<(std::ostream& os, Relation r);
public:
    std::string rname;
    bool is_horizontal;
    int num_of_recs;
    std::vector<Attribute> attributes;
    std::vector<Fragment> frags;

    Relation(std::string rname, bool is_horizontal);
    void add_attribute(Attribute a);
    void add_attribute(std::string aname, bool is_key, int type);
    void add_attribute(std::string aname, bool is_key, int type, int value_type, std::vector<double> value);
    void add_attribute(std::string aname, bool is_key, int type, int value_type, std::map<int, double> proportion);
    void add_fragment(Fragment f);
    void add_fragment(std::string rname, std::string fname, std::string sname, bool is_horizontal, std::vector<std::string> vf_condition);
    void add_fragment(std::string rname, std::string fname, std::string sname, bool is_horizontal, std::vector<Predicate> hf_condition);
    void print_fragments();

    void set_num_of_recs(int n);
};

#endif