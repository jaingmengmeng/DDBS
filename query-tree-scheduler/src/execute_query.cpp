#include <gflags/gflags.h>
#include "brpc/channel.h"
#include "butil/logging.h"

#include <iostream>

#include "ddb.pb.h"

DEFINE_string(temp_table, "1-temp-table", "Temp table name.");
DEFINE_string(protocol, "baidu_std", "Protocol type. Defined in src/brpc/options.proto");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "0.0.0.0:8000", "IP Address of server");
DEFINE_string(load_balancer, "", "The algorithm for load balancing");
DEFINE_int32(timeout_ms, 100, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_int32(interval_ms, 1000, "Milliseconds between consecutive requests");


void HandleEchoResponse(
        brpc::Controller* cntl,
        TableResponse* response) {
    // std::unique_ptr makes sure cntl/response will be deleted before returning.
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    std::unique_ptr<TableResponse> response_guard(response);

    if (cntl->Failed()) {
        LOG(WARNING) << "Some site was down, " << cntl->ErrorText();
        return;
    }
    LOG(INFO) << "Received response from " << cntl->remote_side() << "\nLatency(us): " << cntl->latency_us();
    std::cout << "Result meta: " << response->attr_meta() << std::endl;
    std::cout << "Result: " << std::endl;
    for (int i = 0; i < response->attr_values_size(); ++i) {
        std::cout << response->attr_values(i) << std::endl;
    }
}

int main(int argc, char* argv[]){

// Parse gflags. We recommend you to use gflags as well.
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    // A Channel represents a communication line to a Server. Notice that
    // Channel is thread-safe and can be shared by all threads in your program.
    brpc::Channel channel;

    // Initialize the channel, NULL means using default options.
    brpc::ChannelOptions options;
    options.protocol = FLAGS_protocol;
    options.connection_type = FLAGS_connection_type;
    options.timeout_ms = FLAGS_timeout_ms/*milliseconds*/;
    options.max_retry = FLAGS_max_retry;
    if (channel.Init(FLAGS_server.c_str(), FLAGS_load_balancer.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return -1;
    }

    // Normally, you should not call a Channel directly, but instead construct
    // a stub Service wrapping it. stub can be shared by all threads as well.
    DDBService_Stub stub(&channel);


    // async client
    // todo : add multi-thread implementation
    {
        brpc::Controller* cntl = new brpc::Controller;
        TableResponse* response = new TableResponse;

        TableRequest request;
        request.set_temp_table_name(FLAGS_temp_table);

        google::protobuf::Closure* done = brpc::NewCallback(
                &HandleEchoResponse, cntl, response);

        stub.RequestTable(cntl, &request, response, done);
    }

    return 0;
}