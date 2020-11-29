#include "Attribute.h"

Attribute::Attribute(std::string aname, bool is_key, int type) : 
aname(aname),  is_key(is_key), type(type) {}

Attribute::Attribute(std::string aname, bool is_key, int type, int value_type, std::vector<double> value) :
aname(aname),  is_key(is_key), type(type), value_type(value_type), value(value) {}

Attribute::Attribute(std::string aname, bool is_key, int type, int value_type, std::map<std::string, double> proportion) :
aname(aname),  is_key(is_key), type(type), value_type(value_type), proportion(proportion) {}

std::ostream& operator<<(std::ostream& os, Attribute a) {
    os << a.aname;
    switch (a.type) {
        case 1:
            os << std::string(" int");
            break;
        case 2:
            os << std::string(" string");
            break;
        default:
            break;
    }
    if(a.is_key)
        os << std::string(" key");
    return os;
}
