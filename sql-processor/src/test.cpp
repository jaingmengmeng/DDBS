#include <iostream>
#include <string>
#include <vector>

#include "hsql/SQLParser.h"
#include "hsql/util/sqlhelper.h"

int main(int argc, char *argv[]) {
    // if (argc <= 1) {
    //     fprintf(stderr, "Usage: ./example \"SELECT * FROM test;\"\n");
    //     return -1;
    // }
    std::string query = "select Customer.name, Book.title, Publisher.name, Orders.quantity from Customer, Book, Publisher, Orders where Customer.id=Orders.customer_id and Book.id=Orders.book_id and Book.publisher_id=Publisher.id and Customer.id>308000 and Book.copies>100 and Orders.quantity>1 and Publisher.nation='PRC'";
    // std::string query = R"(SHOW DATABASES)";

    // parse a given query
    hsql::SQLParserResult result;
    hsql::SQLParser::parseSQLString(query, &result);
 
    // check whether the parsing was successful
    if (result.isValid()) {
        printf("Parsed successfully!\n");
        printf("Number of statements: %lu\n", result.size());

        for (int i = 0; i < result.size(); i++)
        {
            hsql::SQLStatement* stat = result.getStatements()[i];
            hsql::printStatementInfo(stat);
            switch (stat->type())
            {
            case hsql::kStmtSelect:
            {
                hsql::SelectStatement* s = static_cast<hsql::SelectStatement*>(stat);
                hsql::printSelectStatementInfo(s, 0);
                // std::cout << "select list:" << std::endl;
                // for (const hsql::Expr* expr : *s->selectList)
                // {
                //     std::cout << expr->getName() << "\t"; 
                // }
                // std::cout << std::endl;
                // std::cout << "from list:" << std::endl;
                // for(const hsql::TableRef* t : *s->fromTable->list){
                //     std::cout << t->getName() << "\t";
                // }
                // std::cout << std::endl;
                // std::cout << "predicate list:" << std::endl;
                // for (const hsql::Expr* expr : *s->whereClause->exprList)
                // {
                //     auto expr1 = expr->expr;
                //     if (expr1->isType(hsql::kExprColumnRef))
                //     {
                //         std::cout << expr1->getName() << "\t";
                //     }
                //     else if (expr1->isType(hsql::kExprLiteralString))
                //     {
                //         std::cout << expr1->getName() << "\t";
                //     }
                //     else if (expr1->isType(hsql::kExprLiteralInt))
                //     {
                //         std::cout << expr1->ival << "\t";
                //     }
                //     else if (expr1->isType(hsql::kExprLiteralFloat))
                //     {
                //         std::cout << expr1->fval << "\t";
                //     }
                    
                    
                    
                //     std::cout << expr->opType << "\t";
                //     auto expr2 = expr->expr2;
                //     if (expr2->isType(hsql::kExprColumnRef))
                //     {
                //         std::cout << expr2->getName() << "\n";
                //     }
                //     else if (expr2->isType(hsql::kExprLiteralString))
                //     {
                //         std::cout << expr2->getName() << "\n";
                //     }
                //     else if (expr2->isType(hsql::kExprLiteralInt))
                //     {
                //         std::cout << expr2->ival << "\n";
                //     }
                //     else if (expr2->isType(hsql::kExprLiteralFloat))
                //     {
                //         std::cout << expr2->fval << "\n";
                //     }
                // }
                break;
            }
            case hsql::kStmtInsert:
                break;
            case hsql::kStmtDelete:
                break;
            
            default:
                break;
            }
        }
        


        // process the statements...
    } else {
        printf("The SQL string is invalid!\n");
        return -1;
    }

    return 0;
}