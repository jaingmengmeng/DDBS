#include "Predicate.h"

Predicate::Predicate(int op_type, std::string aname, double num) :
op_type(op_type), aname(aname), num(num) {}

Predicate::Predicate(int op_type, std::string aname, std::string str) :
op_type(op_type), aname(aname), str(str) {}

std::string double2string(double num) {
    std::ostringstream myos;
    myos << num;
    std::string result = myos.str();
    return result;
}

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
            // os << p.aname << std::string(" = ") << p.join;
            break;
        default:
            break;
        }
    return os;
}
