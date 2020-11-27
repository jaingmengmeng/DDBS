#include "Fragment.h"

Fragment::Fragment(std::string rname, std::string fname, std::string sname, bool is_horizontal, std::vector<std::string> vf_condition) :
rname(rname), fname(fname), sname(sname), is_horizontal(is_horizontal), vf_condition(vf_condition) {}

Fragment::Fragment(std::string rname, std::string fname, std::string sname, bool is_horizontal, std::vector<Predicate> hf_condition) :
rname(rname), fname(fname), sname(sname), is_horizontal(is_horizontal), hf_condition(hf_condition) {}

std::ostream& operator<<(std::ostream& os, Fragment f) {
    os << f.fname << std::string("\t") << f.rname << std::string("\t") << f.sname << std::string("\t");
    if(f.is_horizontal) {
        for(int i=0; i<f.hf_condition.size(); ++i) {
            if(i > 0)
                os << std::string(" AND ");
            os << f.hf_condition[i];
        }
    } else {
        os << std::string("(");
        for(int i=0; i<f.vf_condition.size(); ++i) {
            if(i > 0)
                os << std::string(", ");
            os << f.vf_condition[i];
        }
        os << std::string(")");
    }
    return os;
}
