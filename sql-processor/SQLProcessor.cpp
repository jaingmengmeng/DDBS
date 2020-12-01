#include <vector>
#include <string>
#include <iostream>

#include "SQLProcessor.h"

SQLProcessor::SQLProcessor(std::string q, std::vector<Relation*> relations) :
query(q), relations(relations) {
    this->is_valid = true;  // the sql is valid by default.
    hsql::SQLParser::parseSQLString(this->query, &this->result);
    if (this->result.isValid()) {
        // hsql::SQLStatement*
        this->stat = this->result.getStatements()[0];
        // hsql::printStatementInfo(this->stat);
        switch (this->stat->type()) {
            // select statement
            case hsql::kStmtSelect:
                this->sql_type = 1;
                // static_cast to hsql::SelectStatement*
                this->select_stat = static_cast<hsql::SelectStatement*>(this->stat);
                // generate select_list
                if(this->select_stat->selectList) {
                    for (const hsql::Expr* expr : *this->select_stat->selectList) {
                        std::string rname;
                        std::string aname;
                        // eg. select * from user;
                        if(expr->type == hsql::kExprStar) {
                            // eg. select * from user;
                            if(this->select_stat->fromTable->type == hsql::kTableName) {
                                rname = lower_string(this->select_stat->fromTable->getName());
                                if(this->exist_relation(rname)) {
                                    std::vector<std::string> anames = this->get_anames(rname);
                                    for(auto aname : anames) {
                                        this->select.add_select(rname + std::string("_") + lower_string(aname));
                                    }
                                } else {
                                    // the relation does not exist.
                                    this->is_valid = false;
                                }
                            }
                            // eg. select * from user,book;
                            else if(this->select_stat->fromTable->type == hsql::kTableCrossProduct) {
                                for(const hsql::TableRef* t : *this->select_stat->fromTable->list){
                                    rname = lower_string(t->getName());
                                    if(this->exist_relation(rname)) {
                                        std::vector<std::string> anames = this->get_anames(rname);
                                        for(auto aname : anames) {
                                            this->select.add_select(rname + std::string("_") + lower_string(aname));
                                        }
                                    } else {
                                        // the relation does not exist.
                                        this->is_valid = false;
                                    }
                                }
                            }
                        }
                        // eg. select name,id from user;
                        else if(expr->type == hsql::kExprColumnRef) {
                            if(expr->hasTable()) {
                                // eg. select user.id from user;
                                rname = lower_string(expr->table);
                            } else {
                                // eg. select id from user;
                                if(this->select_stat->fromTable->type == hsql::kTableName) {
                                    rname = lower_string(this->select_stat->fromTable->getName());
                                }
                                // [TODO] Column 'aname' in field list is ambiguous
                            }
                            if(this->exist_relation(rname)) {
                                aname = lower_string(expr->getName());
                                if(this->exist_attribute(rname, aname)) {
                                    this->select.add_select(lower_string(rname) + std::string("_") + lower_string(aname));
                                } else {
                                    // the attribute does not exist.
                                    this->is_valid = false;
                                }
                            } else {
                                // the relation does not exist.
                                this->is_valid = false;
                            }
                        }
                    }
                }

                // generate from_list
                if(this->is_valid && this->select_stat->fromTable) {
                    // eg. select * from user;
                    std::string rname;
                    if(this->select_stat->fromTable->type == hsql::kTableName) {
                        rname = lower_string(this->select_stat->fromTable->getName());
                        if(this->exist_relation(rname)) {
                            this->select.add_from(rname);
                        } else {
                            // the relation does not exist.
                            this->is_valid = false;
                        }
                    }
                    // eg. select * from user,book where book.user_id=user.id;
                    else if(this->select_stat->fromTable->type == hsql::kTableCrossProduct) {
                        for(const hsql::TableRef* t : *this->select_stat->fromTable->list){
                            rname = lower_string(t->getName());
                            if(this->exist_relation(rname)) {
                                this->select.add_from(rname);
                            } else {
                                // the relation does not exist.
                                this->is_valid = false;
                            }
                        }
                    }
                }

                // generate where_clause
                if(this->is_valid && this->select_stat->whereClause) {
                    hsql::Expr* expr = this->select_stat->whereClause;
                    // std::cout << expr->type << std::endl;
                    // Operator 8, (only this type in test cases)
                    if(expr->type == hsql::kExprOperator) {
                        // solve the expr recursively
                        this->solve_expr(expr);
                    }
                }

                if(this->is_valid) {
                    std::cout << this->select << std::endl;
                }
                break;

            // insert statement
            case hsql::kStmtInsert:
                this->sql_type = 2;
                std::cout << "insert" << std::endl;
                break;

            // delete statement
            case hsql::kStmtDelete:
                this->sql_type = 3;
                std::cout << "delete" << std::endl;
                break;

            // show statement. eg. show tables;
            // case hsql::kStmtShow:
            //     this->sql_type = 4;
            //     std::cout << "show" << std::endl;
            //     break;

            default:
                std::cout << "Now the system only supports insert, select and delete statements." << std::endl;
                break;
        }
    } else {
        std::cout << this->result.errorMsg() << std::endl;
    }
}

// solve the expr recursively
void SQLProcessor::solve_expr(hsql::Expr* expr) {
    // AND 19
    if(expr->opType == hsql::kOpAnd && this->is_valid) {
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
                    if(aname != "") {
                        this->select.add_where(op_type, aname, str);
                    } else {
                        this->is_valid = false;
                    }
                }
                // eg. 'jiang'=user.name
                else if(expr->expr2->type == hsql::kExprColumnRef && expr->expr->type == hsql::kExprLiteralString) {
                    op_type = 6;
                    // aname = expr->expr2->getName();
                    aname = this->get_aname_from_expr(expr->expr2);
                    str = expr->expr->getName();
                    if(aname != "") {
                        this->select.add_where(op_type, aname, str);
                    } else {
                        this->is_valid = false;
                    }
                }
                // eg. user.id=creator.user_id
                else if(expr->expr->type == hsql::kExprColumnRef && expr->expr2->type == hsql::kExprColumnRef) {
                    op_type = 7;
                    // std::vector<std::string> join{expr->expr->getName(), expr->expr2->getName()};
                    std::vector<std::string> join;
                    aname = this->get_aname_from_expr(expr->expr);
                    std::string bname = this->get_aname_from_expr(expr->expr2);
                    if(aname != "" && bname != "") {
                        join.push_back(aname);
                        join.push_back(bname);
                        this->select.add_where(op_type, join);
                    } else {
                        this->is_valid = false;
                    }
                }
            } else if(expr->expr->getName()) {
                op_type = 5;
                // aname = expr->expr->getName();
                aname = this->get_aname_from_expr(expr->expr);
                num = expr->expr2->ival;
                if(aname != "") {
                    this->select.add_where(op_type, aname, num);
                } else {
                    this->is_valid = false;
                }
            } else {
                op_type = 5;
                // aname = expr->expr2->getName();
                aname = this->get_aname_from_expr(expr->expr2);
                num = expr->expr->ival;
                if(aname != "") {
                    this->select.add_where(op_type, aname, num);
                } else {
                    this->is_valid = false;
                }
            }
        }
        // < 12
        else if(expr->opType == hsql::kOpLess) {
            op_type = 4;
            // aname = expr->expr->getName();
            aname = this->get_aname_from_expr(expr->expr);
            num = expr->expr2->ival;
            if(aname != "") {
                this->select.add_where(op_type, aname, num);
            } else {
                this->is_valid = false;
            }
        }
        // <= 13
        else if(expr->opType == hsql::kOpLessEq) {
            op_type = 2;
            // aname = expr->expr->getName();
            aname = this->get_aname_from_expr(expr->expr);
            num = expr->expr2->ival;
            if(aname != "") {
                this->select.add_where(op_type, aname, num);
            } else {
                this->is_valid = false;
            }
        }
        // > 14
        else if(expr->opType == hsql::kOpGreater) {
            op_type = 3;
            // aname = expr->expr->getName();
            aname = this->get_aname_from_expr(expr->expr);
            num = expr->expr2->ival;
            if(aname != "") {
                this->select.add_where(op_type, aname, num);
            } else {
                this->is_valid = false;
            }
        }
        // >= 15
        else if(expr->opType == hsql::kOpGreaterEq) {
            op_type = 1;
            // aname = expr->expr->getName();
            aname = this->get_aname_from_expr(expr->expr);
            num = expr->expr2->ival;
            if(aname != "") {
                this->select.add_where(op_type, aname, num);
            } else {
                this->is_valid = false;
            }
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
            rname = lower_string(expr->table);
        } else {
            // eg. select id from user;
            if(this->select_stat->fromTable->type == hsql::kTableName) {
                rname = lower_string(this->select_stat->fromTable->getName());
            }
        }
        aname = lower_string(expr->getName());
    }
    if(this->exist_attribute(rname, aname)) {
        return rname + std::string("_") + aname;
    } else {
        return std::string("");
    }
}

std::vector<std::string> SQLProcessor::get_anames(std::string rname) {
    for(auto relation : this->relations) {
        if(relation->rname == rname) {
            return relation->get_anames();
        }
    }
    return std::vector<std::string>{};
}

bool SQLProcessor::exist_relation(std::string rname) {
    for(auto relation : this->relations) {
        if(relation->rname == rname) {
            return true;
        }
    }
    std::cout << "[ERROR] Table '" + rname + "' doesn't exist." << std::endl;
    return false;
}

bool SQLProcessor::exist_attribute(std::string rname, std::string aname) {
    for(auto relation : this->relations) {
        if(relation->rname == rname) {
            for(auto attribute : relation->attributes) {
                if(attribute.aname == aname) {
                    return true;
                }
            }
        }
    }
    std::cout << "[ERROR] Unknown column '" + aname + "' in Table '" + rname + "'." << std::endl;
    return false;
}
