#include "DataLoader.h"

DataLoader::DataLoader() {
    // create Tables;
    Relation* Publisher = new Relation(std::string("Publisher"));
    Relation* Book = new Relation(std::string("Book"));
    Relation* Customer = new Relation(std::string("Customer"));
    Relation* Orders = new Relation(std::string("Orders"));
    this->relations.push_back(Publisher);
    this->relations.push_back(Book);
    this->relations.push_back(Customer);
    this->relations.push_back(Orders);

    // add Attributes
    Publisher->add_attribute(std::string("id"), true, 1, 1, std::vector<double>{100001, 105000});
    Publisher->add_attribute(std::string("name"), false, 2);
    Publisher->add_attribute(std::string("nation"), false, 2);
    std::cout << *Publisher << std::endl;

    Customer->add_attribute(std::string("id"), true, 1, 1, std::vector<double>{300001, 315000});
    Customer->add_attribute(std::string("name"), false, 2);
    std::map<int, double> m; m[1] = 0.4; m[2] = 0.3; m[3] = 0.3;
    Customer->add_attribute(std::string("rank"), false, 1, 4, m);
    std::cout << *Customer << std::endl;

    Book->add_attribute(std::string("id"), true, 1, 1, std::vector<double>{200001, 250000});
    Book->add_attribute(std::string("title"), false, 2);
    Book->add_attribute(std::string("authors"), false, 2);
    Book->add_attribute(std::string("publisher_id"), false, 1, 1, std::vector<double>{300001, 315000});
    Book->add_attribute(std::string("copies"), false, 1, 2, std::vector<double>{0, 10000});
    std::cout << *Book << std::endl;

    Orders->add_attribute(std::string("customer_id"), true, 1, 1, std::vector<double>{300001, 315000});
    Orders->add_attribute(std::string("book_id"), true, 1, 1, std::vector<double>{200001, 250000});
    Orders->add_attribute(std::string("quantity"), true, 1, 3, std::vector<double>{3, 2});
    std::cout << *Orders << std::endl;

    // add Fragments
}

void DataLoader::init() {
    std::cout << "init" << std::endl;
}
