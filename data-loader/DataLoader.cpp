#include "DataLoader.h"

DataLoader::DataLoader() {
    this->key_map.insert(std::pair<std::string, std::string>("ddbs", "ddbs"));
    this->key_map.insert(std::pair<std::string, std::string>("site_num", "ddbs" + this->sep + "nums_of_sites"));
    this->key_map.insert(std::pair<std::string, std::string>("sites", "ddbs" + this->sep + "sites"));
    this->get_sites();
}

void DataLoader::init() {
    if(delete_from_etcd_by_prefix(this->key_map["ddbs"]) == 0) {
        this->sites.clear();
        std::cout << "Init successfully.\n" << std::endl;
    } else {
        std::cout << "Init failed.\n" << std::endl;
    }
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
                split_string(str, v_str, std::string("\t"));
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

std::vector<std::string> DataLoader::import_fragmented_data(std::string sname, std::string rname) {
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

std::vector<std::string> DataLoader::import_data(std::string rname) {
    std::vector<std::string> res;
    std::vector<std::vector<std::string>> data = this->datas[rname];
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

std::string DataLoader::import_fragmented_data_sql(std::string sname, std::string rname, std::string file_path) {
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

std::string DataLoader::import_data_sql(std::string rname, std::string file_path) {
    std::string res = std::string("");
    std::vector<std::vector<std::string>> data = this->datas[rname];
    Relation* r = this->get_relation(rname);
    if(data.size() == 0) return res;
    res += std::string("INSERT INTO ");
    res += r->rname;
    res += std::string("(");
    for(int i=0; i<r->attributes.size(); ++i) {
        if(i > 0) {
            res += std::string(", ");
        }
        res += r->attributes[i].aname;
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

void DataLoader::show_sites() {
    for(auto site : this->sites) {
        std::cout << site << std::endl;
    }
    std::cout << std::endl;
}

void DataLoader::get_sites() {
    int site_num = this->read_site_num_from_etcd();
    std::string prefix = this->key_map["sites"];
    std::unordered_map<std::string, std::string> site_info = read_from_etcd_by_prefix(prefix);
    for(int i=0; i<site_num; ++i) {
        prefix = this->key_map["sites"] + this->sep+std::to_string(i) + this->sep;
        std::string sname = site_info[prefix+"sname"];
        std::string ip = site_info[prefix+"ip"];
        std::string port = site_info[prefix+"port"];
        this->sites.push_back(Site(sname, ip, port));
    }
}

void DataLoader::add_site(std::string site) {
    // get sname, ip, port
    site = trim(site);
    std::string sname = trim(site.substr(0, site.find_first_of(" ")-0));
    std::string ip =  trim(site.substr(site.find_first_of(" ")+1, site.find_first_of(":")-site.find_first_of(" ")-1));
    std::string port =  trim(site.substr(site.find(":")+1));
    // read_site_num_from_etcd
    int old_site_num = this->read_site_num_from_etcd();
    int new_site_num = old_site_num + 1;
    // write_map_to_etcd
    std::map<std::string, std::string> m;
    m[this->key_map["site_num"]] = std::to_string(new_site_num);
    std::string prefix = this->key_map["sites"] + this->sep+std::to_string(old_site_num) + this->sep;
    m[prefix+"sname"] = sname;
    m[prefix+"ip"] = ip;
    m[prefix+"port"] = port;
    if(write_map_to_etcd(m) == 0) {
        this->sites.push_back(Site(sname, ip, port));
        std::cout << "Add site " << sname << " " << ip << ":" << port << " successfully.\n" << std::endl;
    } else {
        std::cout << "Add site failed.\n" << std::endl;
    }
}

int DataLoader::read_site_num_from_etcd() {
    std::string site_num = read_from_etcd_by_key(this->key_map["site_num"]);
    if(site_num == "") {
        int result = write_kv_to_etcd(this->key_map["site_num"], "0");
        return 0;
    }
    else {
        return std::stoi(site_num);
    }
}
