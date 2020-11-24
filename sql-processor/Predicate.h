#ifndef PREDICATE_H
#define PREDICATE_H

#include<vector>
#include<string>

class Predicate{
public:
    int op_type; //1:>= 2:<= 3:> 4:< 5:=(num) 6:=(string) 7:join
    std::string aname; // Name of attribute e.g.customer_rank
    double num;
    std::string str;
    std::vector<std::string> join;  //if op_type = 7 e.g.customer_id order_cid
    void get_interval(std::vector<double>& result);
};

#endif