#include <vector>
#include <string>
#include <iostream>

#include "SQLProcessor.h"

SQLProcessor::SQLProcessor(std::string q) {
    this->query = q;
    hsql::SQLParser::parseSQLString(this->query, &this->result);
    if (this->result.isValid()) {
        this->stat = this->result.getStatements()[0];
        std::cout << this->stat->type() << std::endl;
        hsql::printStatementInfo(this->stat);
        switch (this->stat->type()) {
            // select statement
            case hsql::kStmtSelect:
                std::cout << "select" << std::endl;
                // SelectStatement select_stat = SelectStatement(this->stat);
                break;
            // insert statement
            case hsql::kStmtInsert:
                std::cout << "insert" << std::endl;
                break;
            // delete statement
            case hsql::kStmtDelete:
                std::cout << "delete" << std::endl;
                break;
            default:
                std::cout << "Now the system only supports insert, select and delete." << std::endl;
                break;
        }
    } else {
        std::cout << this->result.errorMsg() << std::endl;
    }
}

bool SQLProcessor::isValid() {
    return this->result.isValid();
}