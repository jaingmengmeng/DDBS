#include "Site.h"

Site::Site(std::string sname, std::string ip_address, std::string port) :
sname(sname), ip_address(ip_address), port(port) {}

std::string Site::get_url() {
    return this->ip_address + std::string(":") + this->port;
}
