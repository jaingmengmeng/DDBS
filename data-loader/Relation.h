#ifndef RELATION_H
#define RELATION_H

#include <vector>
#include <string>

#include "Fragment.h"
#include "Attribute.h"

class Relation {
private:
    friend std::ostream& operator<<(std::ostream& os, Relation r);
public:
    std::string rname;
    bool is_horizontal;
    int num_of_recs;
    std::vector<Fragment> frags;
    std::vector<Attribute> attributes;

    Relation(std::string rname);
    void add_attribute(Attribute a);
    void add_attribute(std::string aname, bool is_key, int type);
    void add_attribute(std::string aname, bool is_key, int type, int value_type, std::vector<double> value);
    void add_attribute(std::string aname, bool is_key, int type, int value_type, std::map<int, double> proportion);
    void add_fragment(Fragment f);
};

#endif