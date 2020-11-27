#include <vector>
#include <string>
#include <iostream>

#include "SQLProcessor.h"

SQLProcessor::SQLProcessor(std::string q) {
    this->query = q;
    hsql::SQLParser::parseSQLString(this->query, &this->result);
    if (this->result.isValid()) {
        // hsql::SQLStatement*
        this->stat = this->result.getStatements()[0];
        // std::cout << this->stat->type() << std::endl;
        // hsql::printStatementInfo(this->stat);
        hsql::SelectStatement* select_stat;
        switch (this->stat->type()) {
            // select statement
            case hsql::kStmtSelect:
                std::cout << "select" << std::endl;
                // static_cast to hsql::SelectStatement*
                select_stat = static_cast<hsql::SelectStatement*>(this->stat);

                // generate select_list
                if(select_stat->selectList) {
                    for (const hsql::Expr* expr : *select_stat->selectList) {
                        // eg. select * from user;
                        if(expr->type == hsql::kExprStar) {
                            this->select.add_select("*");
                        }
                        // eg. select name from user;
                        else if(expr->type == hsql::kExprColumnRef) {
                            this->select.add_select(expr->getName());
                        }
                    }
                }

                // generate from_list
                if(select_stat->fromTable) {
                    std::cout << select_stat->fromTable->type << std::endl;
                    // eg. select * from user;
                    if(select_stat->fromTable->type == hsql::kTableName) {
                        this->select.add_from(select_stat->fromTable->getName());
                    }
                    // eg. select * from user,book where book.user_id=user.id;
                    else if(select_stat->fromTable->type == hsql::kTableCrossProduct) {
                        for(const hsql::TableRef* t : *select_stat->fromTable->list){
                            this->select.add_from(t->getName());
                        }
                    }
                }

                // generate where_clause
                if(select_stat->whereClause) {
                    std::cout << select_stat->whereClause->type << std::endl;
                    // eg. select * from user,book where book.user_id=user.id;
                    if(select_stat->whereClause->type == hsql::kExprOperator) {
                        std::cout << select_stat->whereClause->opType << std::endl;
                        // =
                        if(select_stat->whereClause->opType == hsql::kOpEquals) {

                        }
                    }
                }

                std::cout << this->select << std::endl;
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


const char *SQLProcessor::errorMsg() {
    return this->result.errorMsg();
}