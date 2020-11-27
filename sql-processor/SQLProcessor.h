#ifndef SQLPROCESSOR_H
#define SQLPROCESSOR_H

#include <vector>
#include <string>

// #include "hsql/SQLParser.h"
// #include "hsql/util/sqlhelper.h"
#include "../../third-party/sql-parser/include/hsql/SQLParser.h"
#include "../../third-party/sql-parser/include/hsql/util/sqlhelper.h"
#include "SelectStatement.h"

class SQLProcessor {
private:
    // void SelectStatement() {}
public:
    std::string query;
    hsql::SQLParserResult result;
    hsql::SQLStatement* stat;
    SelectStatement select;

    SQLProcessor(std::string q);
    bool isValid();
    const char *errorMsg();
};

#endif