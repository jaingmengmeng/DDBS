#include <vector>
#include <unordered_map>

// etcd
int write_kv_to_etcd(const std::string& key, const std::string& value);
int write_map_to_etcd(const std::unordered_map<std::string, std::string>& mp);
std::string read_from_etcd_by_key(const std::string& key);
std::unordered_map<std::string, std::string> read_from_etcd_by_prefix(const std::string& prefix);

// rpc
int load_table(const std::string& host, const std::string& table_name, const std::string& attr_meta, const std::vector<std::string>& attr_values);
std::vector<std::string> request_table(const std::string& temp_table_name);
std::unordered_map<std::string, std::string> get_request_statistics(const std::vector<std::string>& temp_table_names);