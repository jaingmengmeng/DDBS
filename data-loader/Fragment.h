#ifndef FRAGMENT_H
#define FRAGMENT_H

#include <vector>
#include <string>

#include "Predicate.h"

class Fragment {
public:
    std::string rname; //relation name
    std::string fname; //e.g.cus1 cus2 ord1 ord2 ord3 ord4 pub1 ...
    std::string sname; //site name
    bool is_horizontal;
    std::vector<std::string> vf_condition; //e.g. customer_id customer_name
    std::vector<Predicate> hf_condition;
};

#endif