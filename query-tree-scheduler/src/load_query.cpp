#include <string>
#include <vector>
#include <unordered_map>

#include "gflags/gflags.h"
#include "brpc/channel.h"

#include "json.hpp"
#include "base64.h"

DEFINE_string(load_balancer, "", "The algorithm for load balancing");
DEFINE_int32(timeout_ms, 2000, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_string(protocol, "http", "Client-side protocol");

// node type
const std::string LEAF = "L";
const std::string NON_LEAF = "NL";

// combine op
const std::string UNION = "U";
const std::string JOIN = "J";

// seperator
const std::string OUTER_SEPERATOR = "|";
const std::string INNER_SEPERATOR = ":";

const std::string ETCD_PUT_URL = "http://127.0.0.1:2379/v3/kv/put";

const std::string TEMP_TABLE_PREFIX =  "temp-table";

int load_temp_table(brpc::Channel* channel, brpc::Controller* cntl, std::unordered_map<std::string, std::string> mp){
    for (auto iter = mp.cbegin(); iter != mp.cend(); iter++) {
        std::string key = iter->first;
        std::string value = iter->second;
        LOG(INFO) << "put <" << key << "," << value << ">";
        nlohmann::json j;
        j["key"] = base64_encode(key);
        j["value"] = base64_encode(value);
        cntl->http_request().uri() = ETCD_PUT_URL;
        cntl->http_request().set_method(brpc::HTTP_METHOD_POST);
        cntl->request_attachment().append(j.dump());
        channel->CallMethod(NULL, cntl, NULL, NULL, NULL);
        if(cntl->Failed()){
            LOG(ERROR) << cntl->ErrorText() << std::endl;
            return -1;
        }
        cntl->Reset();
    }
    return 0;
}

// load query 9
int load_query_for_test(){

    // 1.prepare channel
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = FLAGS_protocol;
    options.timeout_ms = FLAGS_timeout_ms;
    options.max_retry = FLAGS_max_retry;

    if (channel.Init(ETCD_PUT_URL.c_str(), FLAGS_load_balancer.c_str(), &options) != 0){
        LOG(ERROR) << "Fail to initialize channel";
        return -1;
    }

    brpc::Controller cntl;

    // 2.write query tree info to etcd
    std::unordered_map<std::string, std::string> kv_mp;
    std::unordered_map<int, std::string> site_ip_mp;
    site_ip_mp[1] = "10.77.70.172";
    site_ip_mp[2] = "10.77.70.188";
    site_ip_mp[3] = "10.77.70.189";
    site_ip_mp[4] = "10.77.70.189";

    std::vector<std::string> temp;
    int unique_query_id = 1;
    std::string prefix = "/" + std::to_string(unique_query_id) + "-" + TEMP_TABLE_PREFIX;

    // temp-table
    kv_mp[prefix + "/type"] = NON_LEAF;
    kv_mp[prefix + "/combine"] = UNION;
    kv_mp[prefix + "/children"] = TEMP_TABLE_PREFIX + "-1-1" + INNER_SEPERATOR + site_ip_mp[2] + OUTER_SEPERATOR + TEMP_TABLE_PREFIX + "-1-2" + INNER_SEPERATOR + site_ip_mp[4];
    load_temp_table(&channel, &cntl, kv_mp);
    kv_mp.clear();

    // temp-table-1.1
    kv_mp[prefix + "-1-1" + "/type"] = NON_LEAF;
    kv_mp[prefix + "-1-1" + "/project"] = "customer_name, book_title, publisher_name, order_quantity";
    kv_mp[prefix + "-1-1" + "/combine"] = TEMP_TABLE_PREFIX + "-1-1-1." + "book_id = " + TEMP_TABLE_PREFIX + "-1-1-2." + "order_book_id";
    kv_mp[prefix + "-1-1" + "/children"] = TEMP_TABLE_PREFIX + "-1.1.1" + INNER_SEPERATOR + site_ip_mp[2] + OUTER_SEPERATOR + TEMP_TABLE_PREFIX + "-1.1.2" + INNER_SEPERATOR + site_ip_mp[2];
    load_temp_table(&channel, &cntl, kv_mp);
    kv_mp.clear();

    // temp-table-1.2
    kv_mp[prefix + "-1-2" + "/type"] = NON_LEAF;
    kv_mp[prefix + "-1-2" + "/project"] = "customer_name, book_title, publisher_name, order_quantity";
    kv_mp[prefix + "-1-2" + "/combine"] = TEMP_TABLE_PREFIX + "-1-2-1." + "order_book_id = " + TEMP_TABLE_PREFIX + "-1-2-2." + "book_id";
    kv_mp[prefix + "-1-2" + "/children"] = TEMP_TABLE_PREFIX + "-1-2-1" + INNER_SEPERATOR + site_ip_mp[4] + OUTER_SEPERATOR + TEMP_TABLE_PREFIX + "-1-2-2" + INNER_SEPERATOR + site_ip_mp[4];
    load_temp_table(&channel, &cntl, kv_mp);
    kv_mp.clear();

    // temp-table-1.1.1
    kv_mp[prefix + "-1-1-1" + "/type"] = NON_LEAF;
    kv_mp[prefix + "-1-1-1" + "/combine"] = TEMP_TABLE_PREFIX + "-1-1-1-1." + "book_publisher_id = " + TEMP_TABLE_PREFIX + "-1-1-1-2." + "publisher_id";
    kv_mp[prefix + "-1-1-1" + "/children"] = TEMP_TABLE_PREFIX + "-1-1-1-1" + INNER_SEPERATOR + site_ip_mp[3] + OUTER_SEPERATOR + TEMP_TABLE_PREFIX + "-1-1-1-2" + INNER_SEPERATOR + site_ip_mp[2];
    load_temp_table(&channel, &cntl, kv_mp);
    kv_mp.clear();

    // temp-table-1.1.2
    kv_mp[prefix + "-1-1-2" + "/type"] = NON_LEAF;
    kv_mp[prefix + "-1-1-2" + "/combine"] = TEMP_TABLE_PREFIX + "-1-1-2-1." + "order_customer_id = " + TEMP_TABLE_PREFIX + "-1-1-2-2." + "customer_id";
    kv_mp[prefix + "-1-1-2" + "/children"] = TEMP_TABLE_PREFIX + "-1-1-2-1" + INNER_SEPERATOR + site_ip_mp[2] + OUTER_SEPERATOR + TEMP_TABLE_PREFIX + "-1-1-2-2" + INNER_SEPERATOR + site_ip_mp[1];
    load_temp_table(&channel, &cntl, kv_mp);
    kv_mp.clear();

    // temp-table-1.2.1
    kv_mp[prefix + "-1-2-1" + "/type"] = NON_LEAF;
    kv_mp[prefix + "-1-2-1" + "/combine"] = TEMP_TABLE_PREFIX + "-1-2-1-1." + "order_customer_id = " + TEMP_TABLE_PREFIX + "-1-2-1-2." + "customer_id";
    kv_mp[prefix + "-1-2-1" + "/children"] = TEMP_TABLE_PREFIX + "-1-2-1-1" + INNER_SEPERATOR + site_ip_mp[4] + OUTER_SEPERATOR + TEMP_TABLE_PREFIX + "-1-2-1-2" + INNER_SEPERATOR + site_ip_mp[1];
    load_temp_table(&channel, &cntl, kv_mp);
    kv_mp.clear();

    // temp-table-1.2.2
    kv_mp[prefix + "-1-2-2" + "/type"] = NON_LEAF;
    kv_mp[prefix + "-1-2-2" + "/combine"] = TEMP_TABLE_PREFIX + "-1-2-2-1." + "publisher_id = " + TEMP_TABLE_PREFIX + "-1-2-2-2." + "book_publisher_id";
    kv_mp[prefix + "-1-2-2" + "/children"] = TEMP_TABLE_PREFIX + "-1-2-2-1" + INNER_SEPERATOR + site_ip_mp[4] + OUTER_SEPERATOR + TEMP_TABLE_PREFIX + "-1-2-2-2" + INNER_SEPERATOR + site_ip_mp[3];
    load_temp_table(&channel, &cntl, kv_mp);
    kv_mp.clear();

    // temp-table-1.1.1.1
    kv_mp[prefix + "-1-1-1-1" + "/type"] = LEAF;
    kv_mp[prefix + "-1-1-1-1" + "/project"] = "id as book_id, title as book_title";
    kv_mp[prefix + "-1-1-1-1" + "/select"] = "id > 220000";
    kv_mp[prefix + "-1-1-1-1" + "/children"] = "book";
    load_temp_table(&channel, &cntl, kv_mp);
    kv_mp.clear();

    // temp-table-1.1.1.2
    kv_mp[prefix + "-1-1-1-2" + "/type"] = LEAF;
    kv_mp[prefix + "-1-1-1-2" + "/project"] = "id as publisher_id, name as publisher_name";
    kv_mp[prefix + "-1-1-1-2" + "/children"] = "publisher";
    load_temp_table(&channel, &cntl, kv_mp);
    kv_mp.clear();

    // temp-table-1.1.2.1
    kv_mp[prefix + "-1-1-2-1" + "/type"] = LEAF;
    kv_mp[prefix + "-1-1-2-1" + "/select"] = "quantity > 1";
    kv_mp[prefix + "-1-1-2-1" + "/children"] = "order";
    load_temp_table(&channel, &cntl, kv_mp);
    kv_mp.clear();

    // temp-table-1.1.2.2
    kv_mp[prefix + "-1-1-2-2" + "/type"] = LEAF;
    kv_mp[prefix + "-1-1-2-2" + "/children"] = "customer";
    load_temp_table(&channel, &cntl, kv_mp);
    kv_mp.clear();

    // temp-table-1.2.1.1
    kv_mp[prefix + "-1-2-1-1" + "/type"] = LEAF;
    kv_mp[prefix + "-1-2-1-1" + "/select"] = "quantity > 1";
    kv_mp[prefix + "-1-2-1-1" + "/children"] = "order";
    load_temp_table(&channel, &cntl, kv_mp);
    kv_mp.clear();

    // temp-table-1.2.1.2
    kv_mp[prefix + "-1-2-1-2" + "/type"] = LEAF;
    kv_mp[prefix + "-1-2-1-2" + "/children"] = "customer";
    load_temp_table(&channel, &cntl, kv_mp);
    kv_mp.clear();

    // temp-table-1.2.2.1
    kv_mp[prefix + "-1-2-2-1" + "/type"] = LEAF;
    kv_mp[prefix + "-1-2-2-1" + "/project"] = "id as publisher_id, name as publisher_name";
    kv_mp[prefix + "-1-2-2-1" + "/children"] = "publisher";
    load_temp_table(&channel, &cntl, kv_mp);
    kv_mp.clear();

    // temp-table-1.2.2.2
    kv_mp[prefix + "-1-2-2-2" + "/type"] = LEAF;
    kv_mp[prefix + "-1-2-2-2" + "/project"] = "id as book_id, title as book_title";
    kv_mp[prefix + "-1-2-2-2" + "/select"] = "id > 220000";
    kv_mp[prefix + "-1-2-2-2" + "/children"] = "book";
    load_temp_table(&channel, &cntl, kv_mp);
    kv_mp.clear();
}


int main(int argc, char* argv[]){

    load_query_for_test();

    return 0;
}