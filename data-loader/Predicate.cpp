#include "Predicate.h"

Predicate::Predicate(int op_type, std::string aname, double num) :
op_type(op_type), aname(lower_string(aname)), num(num) {}

Predicate::Predicate(int op_type, std::string aname, std::string str) :
op_type(op_type), aname(lower_string(aname)), str(str) {}

Predicate::Predicate(int op_type, std::vector<std::string> join) :
op_type(op_type), join(join) {}

std::ostream& operator<<(std::ostream& os, Predicate p) {
    switch (p.op_type) {
        case 1:
            os << p.aname << std::string(" >= ") << double2string(p.num);
            break;
        case 2:
            os << p.aname << std::string(" <= ") << double2string(p.num);
            break;
        case 3:
            os << p.aname << std::string(" > ") << double2string(p.num);
            break;
        case 4:
            os << p.aname << std::string(" < ") << double2string(p.num);
            break;
        case 5:
            os << p.aname << std::string(" = ") << double2string(p.num);
            break;
        case 6:
            os << p.aname << std::string(" = '") << p.str << std::string("'");
            break;
        case 7:
            os << p.join[0] << std::string(" = ") << p.join[1];
            break;
        default:
            break;
        }
    return os;
}

bool Predicate::test(std::string value) {
    switch(this->op_type) {
        case 1:
            return std::stod(value) >= this->num;
        case 2:
            return std::stod(value) <= this->num;
        case 3:
            return std::stod(value) > this->num;
        case 4:
            return std::stod(value) > this->num;
        case 5:
            return std::stod(value) == this->num;
        case 6:
            return value == this->str;
        case 8:
            return std::stod(value) != this->num;
        case 9:
            return value != this->str;
        default:
            break;
    }
}
