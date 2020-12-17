#ifndef DATALOADER_H
#define DATALOADER_H

#include <string>
#include <vector>
#include <iostream>
#include <map>
#include <unordered_map>
#include <fstream>

#include "Site.h"
#include "Relation.h"
#include "../utils/utils.h"
#include "../network-utils/network.h"

class DataLoader {
private:
    std::string sep = "/";
    std::map<std::string, std::string> key_map;

    std::map<std::string, std::vector<std::vector<std::string>>> data_fragment(Relation relation);
    Relation get_relation(std::string rname);
public:
    std::vector<Relation> relations;
    std::map<std::string, std::string> files;
    std::vector<Site> sites;
    std::unordered_map<std::string, Fragment> unallocated_fragments;
    std::map<std::string, std::vector<std::vector<std::string>>> datas; // relations
    std::map<std::string, std::map<std::string, std::vector<std::vector<std::string>>>> fragmented_datas;   // relations, sites

    DataLoader();
    void init();    // initial the relations, sites variables.
    void show_tables(bool show_fragment=true);   // show all tables & fragments information.
    void load_data();   // initial (data/fragmented data) from files.
    std::vector<std::string> import_fragmented_data(std::string sname, std::string rname);   // load fragmented data to servers
    std::vector<std::string> import_data(std::string rname);   // load global data to servers
    std::string import_fragmented_data_sql(std::string sname, std::string rname, std::string file_path="");    // load fragmented data to servers via sql
    std::string import_data_sql(std::string rname, std::string file_path="");    // load global data to servers via sql

    void show_sites();
    void get_sites();
    void add_site(std::string sname, std::string ip, std::string port);
    int read_site_num_from_etcd();

    void get_relations();
    void add_relation(std::string rname, std::vector<Attribute> attributes);
    int read_relation_num_from_etcd();

    void add_unallocated_fragment(Fragment fragment);
    void allocate(std::string fname, std::string sname);
    void add_fragment(Fragment fragment);
    void show_unallocated_fragments();

    std::string get_prefix_by_rname(std::string rname);
};

#endif