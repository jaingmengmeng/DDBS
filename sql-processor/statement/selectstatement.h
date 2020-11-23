#include<vector>
#include<string>

class SelectStatement {
private:
    std::vector<std::string> from_table;
    std::vector<std::string> select_list;
    std::vector<Predicate> where_clause;
};