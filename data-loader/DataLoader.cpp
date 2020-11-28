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
    Relation* Book = new Relation(std::string("Book"), true);
    Relation* Customer = new Relation(std::string("Customer"), false);
    Relation* Orders = new Relation(std::string("Orders"), true);
    Relation* Publisher = new Relation(std::string("Publisher"), true);
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
    Orders->add_attribute(std::string("quantity"), false, 1, 3, std::vector<double>{3, 2});
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

std::vector<std::string> tsv_split(const std::string& s, const std::string& c) {
    std::vector<std::string> v;
    v.clear();
    std::string::size_type pos1, pos2;
    pos2 = s.find(c);
    pos1 = 0;
    while(std::string::npos != pos2) {
        v.push_back(s.substr(pos1, pos2-pos1));
        pos1 = pos2 + c.size();
        pos2 = s.find(c, pos1);
    }
    if(pos1 != s.length())
        v.push_back(s.substr(pos1));
    return v;
}

std::string Convert2Insert(Relation* r, std::vector<std::vector<std::string> > v, std::string sname) {
    std::string res = std::string("");
    if(v.size() == 0) return res;
    res += std::string("INSERT INTO ");
    res += r->rname;
    res += std::string("(");
    if(r->is_horizontal) {
        for(int i=0; i<r->attributes.size(); ++i) {
            if(i > 0) {
                res += std::string(", ");
            }
            res += r->attributes[i].aname;
        }
    } else {
        for(auto fragment : r->frags) {
            if(fragment.sname == sname) {
                for(int i=0; i<fragment.vf_condition.size(); ++i) {
                    if(i > 0) {
                        res += std::string(", ");
                    }
                    res += fragment.vf_condition[i];
                }
            }
        }
    }
    res += std::string(") VALUES ");
    for(int i=0; i<v.size(); ++i) {
        if(i > 0) {
            res += std::string(", ");
        }
        res += std::string("(");
        for(int j=0; j<v[i].size(); ++j) {
            if(j > 0) {
                res += std::string(", ");
            }
            if(r->attributes[j].type == 1) {
                res += v[i][j];
            } else if(r->attributes[j].type == 2) {
                res += std::string("'");
                res += v[i][j];
                res += std::string("'");
            }
        }
        res += std::string(")");
    }
    res += std::string(";");
    return res;
}

std::map<std::string, std::vector<std::vector<std::string>>> DataLoader::data_fragment(Relation* relation, std::vector<std::vector<std::string>> datas) {
    std::map<std::string, std::vector<std::vector<std::string>>> allocated_data_map;
    if(relation->is_horizontal) {
        for(auto data : datas) {
            for(auto fragment : relation->frags) {
                std::vector<bool> result;
                for(auto predicate : fragment.hf_condition) {
                    for(int i=0; i<relation->attributes.size(); ++i) {
                        if(relation->attributes[i].aname == predicate.aname) {
                            switch (predicate.op_type) {
                                case 1:
                                    if(atof(data[i].c_str()) >= predicate.num) {
                                        result.push_back(true);
                                    }
                                    break;
                                case 2:
                                    if(atof(data[i].c_str()) <= predicate.num) {
                                        result.push_back(true);
                                    }
                                    break;
                                case 3:
                                    if(atof(data[i].c_str()) > predicate.num) {
                                        result.push_back(true);
                                    }
                                    break;
                                case 4:
                                    if(atof(data[i].c_str()) < predicate.num) {
                                        result.push_back(true);
                                    }
                                    break;
                                case 5:
                                    if(atof(data[i].c_str()) == predicate.num) {
                                        result.push_back(true);
                                    }
                                    break;
                                case 6:
                                    if(data[i] == predicate.str) {
                                        result.push_back(true);
                                    }
                                    break;
                                case 7:
                                    break; 
                                default:
                                    break;
                            }
                        }
                    }
                }
                if(result.size() == fragment.hf_condition.size())
                    allocated_data_map[fragment.sname].push_back(data);
            }
        }
    } else {
        for(auto data : datas) {
            for(auto fragment : relation->frags) {
                std::vector<std::string> tmp;
                for(auto aname : fragment.vf_condition) {
                    for(int i=0; i<relation->attributes.size(); ++i) {
                        if(relation->attributes[i].aname == aname) {
                            tmp.push_back(data[i]);
                        }
                    }
                }
                allocated_data_map[fragment.sname].push_back(tmp);
            }
        }
    }
    return allocated_data_map;
}

void DataLoader::load_data() {
    std::string file_error = std::string("Error opening file. Please check your filename.");
    for(auto relation : this->relations) {
        std::string file_path = this->files[relation->rname];
        std::ifstream fin(file_path, std::ios_base::in);
        std::string str;
        if (fin.is_open()) {
            std::vector<std::vector<std::string>> vv_str;
            while(std::getline(fin, str)) {
                // 根据制表符分割字符串
                std::vector<std::string> v_str = tsv_split(str, std::string("\t"));
                vv_str.push_back(v_str);
            }
            // 根据分片信息将所有数据进行分到不同的site上
            std::map<std::string, std::vector<std::vector<std::string>>> allocated_data_map = this->data_fragment(relation, vv_str);
            std::vector<std::string> insert_str;
            for(auto sname : this->sites) {
                std::string insert = Convert2Insert(relation, allocated_data_map[sname], sname);
                if(insert != std::string("")) {
                    insert_str.push_back(insert);
                    std::ofstream fout(sname+"_data_loader.sql", std::ios::app);
                    fout << insert << std::endl;
                    fout.close();
                }
            }
        } else {
            std::cout << file_error << std::endl;
            exit(1);
        }
    }
}
