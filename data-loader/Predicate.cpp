#include "Predicate.h"

Predicate::Predicate(int op_type, std::string aname, double num) :
op_type(op_type), aname(aname), num(num) {}

Predicate::Predicate(int op_type, std::string aname, std::string str) :
op_type(op_type), aname(aname), str(str) {}
