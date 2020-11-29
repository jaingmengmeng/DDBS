#include "network.h"
#include <iostream>

int main(int argc, char* argv[]){

//    int result = write_kv_to_etcd("/test-key1", "test-value1");
//    std::cout << result << std::endl;
//    std::cout << read_from_etcd_by_key("/test-key1");

    std::string host = "127.0.0.1:8000";
    std::string table_name = "test2";
    std::string attr_meta = "`id` int(11) not null, `name` varchar(20) not null";
    std::vector<std::string> attr_values;
    attr_values.emplace_back("1, 'a'");
    attr_values.emplace_back("2, 'b'");
    attr_values.emplace_back("3, 'c'");

    return load_table(host, table_name, attr_meta, attr_values);

    return 0;
}