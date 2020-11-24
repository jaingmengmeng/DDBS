#ifndef SQLPROCESSOR_H
#define SQLPROCESSOR_H

#include <vector>
#include <string>

#include "sql-parser/include/hsql/SQLParser.h"
#include "sql-parser/include/hsql/util/sqlhelper.h"

class SQLProcessor {
private:
    // void SelectStatement() {}
public:
    std::string query;
    hsql::SQLParserResult result;
    hsql::SQLStatement* stat;
    SQLProcessor(std::string q);
    bool isValid();
};

#endif