#ifndef SQLPROCESSOR_H
#define SQLPROCESSOR_H

#include <vector>
#include <string>

// #include "hsql/SQLParser.h"
// #include "hsql/util/sqlhelper.h"
#include "../../third-party/sql-parser/include/hsql/SQLParser.h"
#include "../../third-party/sql-parser/include/hsql/util/sqlhelper.h"
#include "SelectStatement.h"
#include "../utils/utils.h"

class SQLProcessor {
private:
    hsql::SelectStatement* select_stat;
    hsql::InsertStatement* insert_stat;
    hsql::DeleteStatement* delete_stat;
    hsql::SQLParserResult result;
    hsql::SQLStatement* stat;
public:
    std::string query;
    SelectStatement select;

    SQLProcessor(std::string q);
    bool isValid();
    const char *errorMsg();
    void solve_expr(hsql::Expr* expr);
    std::string get_aname_from_expr(hsql::Expr* expr);
};

#endif