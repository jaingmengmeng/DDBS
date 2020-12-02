#ifndef SITE_H
#define SITE_H

#include <string>
#include <vector>

class Site {
public:
    std::string sname;
    std::string ip_address;
    std::string port;

    Site(std::string sname, std::string ip_address, std::string port);
    std::string get_url();
};

#endif