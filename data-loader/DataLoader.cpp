#include "DataLoader.h"

DataLoader::DataLoader() {
    // initialize file_path;
    this->files[std::string("Book")] = std::string("../files/data/book.tsv");
    this->files[std::string("Customer")] = std::string("../files/data/customer.tsv");
    this->files[std::string("Orders")] = std::string("../files/data/orders.tsv");
    this->files[std::string("Publisher")] = std::string("../files/data/publisher.tsv");
    this->sites.push_back(std::string("site1"));
    this->sites.push_back(std::string("site2"));
    this->sites.push_back(std::string("site3"));
    this->sites.push_back(std::string("site4"));

    // create Tables;
    Relation* Book = new Relation(std::string("Book"));
    Relation* Customer = new Relation(std::string("Customer"));
    Relation* Orders = new Relation(std::string("Orders"));
    Relation* Publisher = new Relation(std::string("Publisher"));
    this->relations.push_back(Book);
    this->relations.push_back(Customer);
    this->relations.push_back(Orders);
    this->relations.push_back(Publisher);

    // add Attributes
    Book->add_attribute(std::string("id"), true, 1, 1, std::vector<double>{200001, 250000});
    Book->add_attribute(std::string("title"), false, 2);
    Book->add_attribute(std::string("authors"), false, 2);
    Book->add_attribute(std::string("publisher_id"), false, 1, 1, std::vector<double>{300001, 315000});
    Book->add_attribute(std::string("copies"), false, 1, 2, std::vector<double>{0, 10000});
    // std::cout << *Book << std::endl;

    Customer->add_attribute(std::string("id"), true, 1, 1, std::vector<double>{300001, 315000});
    Customer->add_attribute(std::string("name"), false, 2);
    std::map<int, double> m; m[1] = 0.4; m[2] = 0.3; m[3] = 0.3;
    Customer->add_attribute(std::string("rank"), false, 1, 4, m);
    // std::cout << *Customer << std::endl;

    Orders->add_attribute(std::string("customer_id"), true, 1, 1, std::vector<double>{300001, 315000});
    Orders->add_attribute(std::string("book_id"), true, 1, 1, std::vector<double>{200001, 250000});
    Orders->add_attribute(std::string("quantity"), true, 1, 3, std::vector<double>{3, 2});
    // std::cout << *Orders << std::endl;

    Publisher->add_attribute(std::string("id"), true, 1, 1, std::vector<double>{100001, 105000});
    Publisher->add_attribute(std::string("name"), false, 2);
    Publisher->add_attribute(std::string("nation"), false, 2);
    // std::cout << *Publisher << std::endl;

    // add Fragments
    std::vector<Predicate> predicates;

    predicates.clear();
    predicates.push_back(Predicate(4, std::string("id"), 205000));
    Book->add_fragment(std::string("Book"), std::string("book1"), this->sites[0], true, predicates);
    predicates.clear();
    predicates.push_back(Predicate(1, std::string("id"), 205000));
    predicates.push_back(Predicate(4, std::string("id"), 210000));
    Book->add_fragment(std::string("Book"), std::string("book2"), this->sites[1], true, predicates);
    predicates.clear();
    predicates.push_back(Predicate(1, std::string("id"), 210000));
    Book->add_fragment(std::string("Book"), std::string("book3"), this->sites[2], true, predicates);
    // Book->print_fragments();

    Customer->add_fragment(std::string("Customer"), std::string("cus1"), this->sites[0], false, std::vector<std::string>{std::string("id"), std::string("name")});
    Customer->add_fragment(std::string("Customer"), std::string("cus2"), this->sites[1], false, std::vector<std::string>{std::string("id"), std::string("rank")});
    // Customer->print_fragments();

    predicates.clear();
    predicates.push_back(Predicate(4, std::string("customer_id"), 307000));
    predicates.push_back(Predicate(4, std::string("book_id"), 215000));
    Orders->add_fragment(std::string("Orders"), std::string("ord1"), this->sites[0], true, predicates);
    predicates.clear();
    predicates.push_back(Predicate(4, std::string("customer_id"), 307000));
    predicates.push_back(Predicate(1, std::string("book_id"), 215000));
    Orders->add_fragment(std::string("Orders"), std::string("ord2"), this->sites[1], true, predicates);
    predicates.clear();
    predicates.push_back(Predicate(1, std::string("customer_id"), 307000));
    predicates.push_back(Predicate(4, std::string("book_id"), 215000));
    Orders->add_fragment(std::string("Orders"), std::string("ord3"), this->sites[2], true, predicates);
    predicates.clear();
    predicates.push_back(Predicate(1, std::string("customer_id"), 307000));
    predicates.push_back(Predicate(1, std::string("book_id"), 215000));
    Orders->add_fragment(std::string("Orders"), std::string("ord4"), this->sites[3], true, predicates);
    // Orders->print_fragments();

    predicates.clear();
    predicates.push_back(Predicate(4, std::string("id"), 104000));
    predicates.push_back(Predicate(6, std::string("nation"), std::string("PRC")));
    Publisher->add_fragment(std::string("Publisher"), std::string("pub1"), this->sites[0], true, predicates);
    predicates.clear();
    predicates.push_back(Predicate(4, std::string("id"), 104000));
    predicates.push_back(Predicate(6, std::string("nation"), std::string("USA")));
    Publisher->add_fragment(std::string("Publisher"), std::string("pub2"), this->sites[1], true, predicates);
    predicates.clear();
    predicates.push_back(Predicate(1, std::string("id"), 104000));
    predicates.push_back(Predicate(6, std::string("nation"), std::string("PRC")));
    Publisher->add_fragment(std::string("Publisher"), std::string("pub3"), this->sites[2], true, predicates);
    predicates.clear();
    predicates.push_back(Predicate(1, std::string("id"), 104000));
    predicates.push_back(Predicate(6, std::string("nation"), std::string("USA")));
    Publisher->add_fragment(std::string("Publisher"), std::string("pub4"), this->sites[3], true, predicates);
    // Publisher->print_fragments();
}

void DataLoader::init() {
    std::cout << std::string("init") << std::endl;
}

void DataLoader::show_tables() {
    for(auto relation : this->relations) {
        std::cout << *relation;
        relation->print_fragments();
    }
}
