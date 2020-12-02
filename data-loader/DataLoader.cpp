#include "DataLoader.h"

DataLoader::DataLoader() {
    this->init();
}

void DataLoader::init() {
    // initialize file_path;
    this->files[std::string("book")] = std::string("../files/data/book.tsv");
    this->files[std::string("customer")] = std::string("../files/data/customer.tsv");
    this->files[std::string("orders")] = std::string("../files/data/orders.tsv");
    this->files[std::string("publisher")] = std::string("../files/data/publisher.tsv");
    this->sites.push_back(Site("site1", "10.77.70.172", "8000"));
    this->sites.push_back(Site("site2", "10.77.70.188", "8000"));
    this->sites.push_back(Site("site3", "10.77.70.189", "8000"));
    this->sites.push_back(Site("site4", "10.77.70.189", "8000"));

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
    std::map<std::string, double> m; m["1"] = 0.4; m["2"] = 0.3; m["3"] = 0.3;
    Customer->add_attribute(std::string("rank"), false, 1, 4, m);
    // std::cout << *Customer << std::endl;

    Orders->add_attribute(std::string("customer_id"), true, 1, 1, std::vector<double>{300001, 315000});
    Orders->add_attribute(std::string("book_id"), true, 1, 1, std::vector<double>{200001, 250000});
    Orders->add_attribute(std::string("quantity"), false, 1, 3, std::vector<double>{3, 2});
    // std::cout << *Orders << std::endl;

    Publisher->add_attribute(std::string("id"), true, 1, 1, std::vector<double>{100001, 105000});
    Publisher->add_attribute(std::string("name"), false, 2);
    std::map<std::string, double> m2; m2["PRC"] = 0.5; m2["USA"] = 0.3;
    Publisher->add_attribute(std::string("nation"), false, 2, 4, m2);
    // std::cout << *Publisher << std::endl;

    // add Fragments
    std::vector<Predicate> predicates;

    predicates.clear();
    predicates.push_back(Predicate(4, std::string("id"), 205000));
    Book->add_fragment(std::string("Book"), std::string("book1"), std::string("site1"), true, predicates);
    predicates.clear();
    predicates.push_back(Predicate(1, std::string("id"), 205000));
    predicates.push_back(Predicate(4, std::string("id"), 210000));
    Book->add_fragment(std::string("Book"), std::string("book2"), std::string("site2"), true, predicates);
    predicates.clear();
    predicates.push_back(Predicate(1, std::string("id"), 210000));
    Book->add_fragment(std::string("Book"), std::string("book3"), std::string("site3"), true, predicates);
    // Book->print_fragments();

    Customer->add_fragment(std::string("Customer"), std::string("cus1"), std::string("site1"), false, std::vector<std::string>{std::string("id"), std::string("name")});
    Customer->add_fragment(std::string("Customer"), std::string("cus2"), std::string("site2"), false, std::vector<std::string>{std::string("id"), std::string("rank")});
    // Customer->print_fragments();

    predicates.clear();
    predicates.push_back(Predicate(4, std::string("customer_id"), 307000));
    predicates.push_back(Predicate(4, std::string("book_id"), 215000));
    Orders->add_fragment(std::string("Orders"), std::string("ord1"), std::string("site1"), true, predicates);
    predicates.clear();
    predicates.push_back(Predicate(4, std::string("customer_id"), 307000));
    predicates.push_back(Predicate(1, std::string("book_id"), 215000));
    Orders->add_fragment(std::string("Orders"), std::string("ord2"), std::string("site2"), true, predicates);
    predicates.clear();
    predicates.push_back(Predicate(1, std::string("customer_id"), 307000));
    predicates.push_back(Predicate(4, std::string("book_id"), 215000));
    Orders->add_fragment(std::string("Orders"), std::string("ord3"), std::string("site3"), true, predicates);
    predicates.clear();
    predicates.push_back(Predicate(1, std::string("customer_id"), 307000));
    predicates.push_back(Predicate(1, std::string("book_id"), 215000));
    Orders->add_fragment(std::string("Orders"), std::string("ord4"), std::string("site4"), true, predicates);
    // Orders->print_fragments();

    predicates.clear();
    predicates.push_back(Predicate(4, std::string("id"), 104000));
    predicates.push_back(Predicate(6, std::string("nation"), std::string("PRC")));
    Publisher->add_fragment(std::string("Publisher"), std::string("pub1"), std::string("site1"), true, predicates);
    predicates.clear();
    predicates.push_back(Predicate(4, std::string("id"), 104000));
    predicates.push_back(Predicate(6, std::string("nation"), std::string("USA")));
    Publisher->add_fragment(std::string("Publisher"), std::string("pub2"), std::string("site2"), true, predicates);
    predicates.clear();
    predicates.push_back(Predicate(1, std::string("id"), 104000));
    predicates.push_back(Predicate(6, std::string("nation"), std::string("PRC")));
    Publisher->add_fragment(std::string("Publisher"), std::string("pub3"), std::string("site3"), true, predicates);
    predicates.clear();
    predicates.push_back(Predicate(1, std::string("id"), 104000));
    predicates.push_back(Predicate(6, std::string("nation"), std::string("USA")));
    Publisher->add_fragment(std::string("Publisher"), std::string("pub4"), std::string("site4"), true, predicates);
    // Publisher->print_fragments();

    this->load_data();
}

void DataLoader::show_tables(bool show_fragment) {
    for(auto relation : this->relations) {
        std::cout << *relation;
        if(show_fragment) {
            relation->print_fragments();
        }
    }
}

std::map<std::string, std::vector<std::vector<std::string>>> DataLoader::data_fragment(Relation* relation) {
    std::map<std::string, std::vector<std::vector<std::string>>> allocated_data_map;
    if(relation->is_horizontal) {
        for(auto data : this->datas[relation->rname]) {
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
                if(result.size() == fragment.hf_condition.size()) {
                    allocated_data_map[fragment.sname].push_back(data);
                }
            }
        }
    } else {
        for(auto data : this->datas[relation->rname]) {
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
    for(auto& fragment : relation->frags) {
        fragment.set_num_of_recs(allocated_data_map[fragment.sname].size());
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
            while(std::getline(fin, str)) {
                // 根据制表符分割字符串
                std::vector<std::string> v_str;
                SplitString(str, v_str, std::string("\t"));
                this->datas[relation->rname].push_back(v_str);
            }
            relation->set_num_of_recs(this->datas[relation->rname].size());
            // 根据分片信息将所有数据分到不同的site上
            this->fragmented_datas[relation->rname] = this->data_fragment(relation);
        } else {
            std::cout << file_error << std::endl;
            exit(1);
        }
    }
}

std::vector<std::string> DataLoader::import_data(std::string sname, std::string rname) {
    std::vector<std::string> res;
    std::vector<std::vector<std::string>> data = this->fragmented_datas[rname][sname];
    Relation* r = this->get_relation(rname);
    if(data.size() == 0) return res;
    for(int i=0; i<data.size(); ++i) {
        std::string values = "";
        for(int j=0; j<data[i].size(); ++j) {
            if(j > 0) {
                values += std::string(", ");
            }
            if(r->attributes[j].type == 1) {
                values += data[i][j];
            } else if(r->attributes[j].type == 2) {
                values += std::string("'");
                values += data[i][j];
                values += std::string("'");
            }
        }
        res.push_back(values);
    }
    return res;
}

std::string DataLoader::import_data_sql(std::string sname, std::string rname, std::string file_path) {
    std::string res = std::string("");
    std::vector<std::vector<std::string>> data = this->fragmented_datas[rname][sname];
    Relation* r = this->get_relation(rname);
    if(data.size() == 0) return res;
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
    for(int i=0; i<data.size(); ++i) {
        if(i > 0) {
            res += std::string(", ");
        }
        res += std::string("(");
        for(int j=0; j<data[i].size(); ++j) {
            if(j > 0) {
                res += std::string(", ");
            }
            if(r->attributes[j].type == 1) {
                res += data[i][j];
            } else if(r->attributes[j].type == 2) {
                res += std::string("'");
                res += data[i][j];
                res += std::string("'");
            }
        }
        res += std::string(")");
    }
    res += std::string(";");
    if(file_path != "") {
        // 如果没有文件则创建文件; 如果有则清空文件.
        std::ofstream fout(file_path, std::ios::ate | std::ios::out);
        fout << res << std::endl;
        fout.close();
    }
    return res;
}

Relation* DataLoader::get_relation(std::string rname) {
    for(auto relation : this->relations) {
        if(relation->rname == rname) {
            return relation;
        }
    }
    return nullptr;
}
