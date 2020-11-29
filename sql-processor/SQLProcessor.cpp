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
        // hsql::printStatementInfo(this->stat);
        switch (this->stat->type()) {
            // select statement
            case hsql::kStmtSelect:
                // static_cast to hsql::SelectStatement*
                this->select_stat = static_cast<hsql::SelectStatement*>(this->stat);
                // generate select_list
                if(this->select_stat->selectList) {
                    for (const hsql::Expr* expr : *this->select_stat->selectList) {
                        std::string rname;
                        std::string aname;
                        // eg. select * from user;
                        if(expr->type == hsql::kExprStar) {
                            aname = std::string("*");
                            if(this->select_stat->fromTable->type == hsql::kTableName) {
                                rname = this->select_stat->fromTable->getName();
                                this->select.add_select(rname + std::string("_") + aname);
                            }
                            // eg. select * from user,book where book.user_id=user.id;
                            else if(this->select_stat->fromTable->type == hsql::kTableCrossProduct) {
                                for(const hsql::TableRef* t : *this->select_stat->fromTable->list){
                                    rname = t->getName();
                                    this->select.add_select(rname + std::string("_") + aname);
                                }
                            }
                        }
                        // eg. select name,id from user;
                        else if(expr->type == hsql::kExprColumnRef) {
                            if(expr->hasTable()) {
                                // eg. select user.id from user;
                                rname = expr->table;
                            } else {
                                // eg. select id from user;
                                if(this->select_stat->fromTable->type == hsql::kTableName) {
                                    rname = this->select_stat->fromTable->getName();
                                }
                            }
                            aname = expr->getName();
                            this->select.add_select(rname + std::string("_") + aname);
                        }
                    }
                }

                // generate from_list
                if(this->select_stat->fromTable) {
                    // eg. select * from user;
                    if(this->select_stat->fromTable->type == hsql::kTableName) {
                        this->select.add_from(this->select_stat->fromTable->getName());
                    }
                    // eg. select * from user,book where book.user_id=user.id;
                    else if(this->select_stat->fromTable->type == hsql::kTableCrossProduct) {
                        for(const hsql::TableRef* t : *this->select_stat->fromTable->list){
                            this->select.add_from(t->getName());
                        }
                    }
                }

                // generate where_clause
                if(this->select_stat->whereClause) {
                    hsql::Expr* expr = this->select_stat->whereClause;
                    // std::cout << expr->type << std::endl;
                    // Operator 8, (only this type in test cases)
                    if(expr->type == hsql::kExprOperator) {
                        // solve the expr recursively
                        this->solve_expr(expr);
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

            // show statement. eg. show tables;
            // case hsql::kStmtShow:
            //     std::cout << "show" << std::endl;
            //     break;

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

void SQLProcessor::solve_expr(hsql::Expr* expr) {
    // AND 19
    if(expr->opType == hsql::kOpAnd) {
        this->solve_expr(expr->expr);
        this->solve_expr(expr->expr2);
    }
    else {
        std::string aname;
        int op_type;
        double num;
        std::string str;
        // = 10
        // eg. user.id=10; user.name='jiang'; user.id=creator.user_id
        if(expr->opType == hsql::kOpEquals) {
            if(expr->expr->getName() && expr->expr2->getName()) {
                // eg. user.name='jiang'
                if(expr->expr->type == hsql::kExprColumnRef && expr->expr2->type == hsql::kExprLiteralString) {
                    op_type = 6;
                    // aname = expr->expr->getName();
                    aname = this->get_aname_from_expr(expr->expr);
                    str = expr->expr2->getName();
                    this->select.add_where(op_type, aname, str);
                }
                // eg. 'jiang'=user.name
                else if(expr->expr2->type == hsql::kExprColumnRef && expr->expr->type == hsql::kExprLiteralString) {
                    op_type = 6;
                    // aname = expr->expr2->getName();
                    aname = this->get_aname_from_expr(expr->expr2);
                    str = expr->expr->getName();
                    this->select.add_where(op_type, aname, str);
                }
                // eg. user.id=creator.user_id
                else if(expr->expr->type == hsql::kExprColumnRef && expr->expr2->type == hsql::kExprColumnRef) {
                    op_type = 7;
                    // std::vector<std::string> join{expr->expr->getName(), expr->expr2->getName()};
                    std::vector<std::string> join{this->get_aname_from_expr(expr->expr), this->get_aname_from_expr(expr->expr2)};
                    this->select.add_where(op_type, join);
                }
            } else if(expr->expr->getName()) {
                op_type = 5;
                // aname = expr->expr->getName();
                aname = this->get_aname_from_expr(expr->expr);
                num = expr->expr2->ival;
                std::cout << expr->expr2->ival << std::endl;
                this->select.add_where(op_type, aname, num);
            } else {
                op_type = 5;
                // aname = expr->expr2->getName();
                aname = this->get_aname_from_expr(expr->expr2);
                num = expr->expr->ival;
                std::cout << expr->expr->ival << std::endl;
                this->select.add_where(op_type, aname, num);
            }
        }
        // < 12
        else if(expr->opType == hsql::kOpLess) {
            op_type = 4;
            // aname = expr->expr->getName();
            aname = this->get_aname_from_expr(expr->expr);
            num = expr->expr2->ival;
            this->select.add_where(op_type, aname, num);
        }
        // <= 13
        else if(expr->opType == hsql::kOpLessEq) {
            op_type = 2;
            // aname = expr->expr->getName();
            aname = this->get_aname_from_expr(expr->expr);
            num = expr->expr2->ival;
            this->select.add_where(op_type, aname, num);
        }
        // > 14
        else if(expr->opType == hsql::kOpGreater) {
            op_type = 3;
            // aname = expr->expr->getName();
            aname = this->get_aname_from_expr(expr->expr);
            num = expr->expr2->ival;
            this->select.add_where(op_type, aname, num);
        }
        // >= 15
        else if(expr->opType == hsql::kOpGreaterEq) {
            op_type = 1;
            // aname = expr->expr->getName();
            aname = this->get_aname_from_expr(expr->expr);
            num = expr->expr2->ival;
            this->select.add_where(op_type, aname, num);
        }
    }
}

std::string SQLProcessor::get_aname_from_expr(hsql::Expr* expr) {
    std::string aname;
    std::string rname;
    // eg. select name,id from user;
    if(expr->type == hsql::kExprColumnRef) {
        if(expr->hasTable()) {
            // eg. select user.id from user;
            rname = expr->table;
        } else {
            // eg. select id from user;
            if(this->select_stat->fromTable->type == hsql::kTableName) {
                rname = this->select_stat->fromTable->getName();
            }
        }
        aname = expr->getName();
    }
    return rname + std::string("_") + aname;
}
