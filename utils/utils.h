#ifndef UTILS_H
#define UTILS_H

#include <string>
#include <vector>
#include <algorithm>
#include <iostream>
#include <sstream>

// extern inline，这里必须设置为内联
inline void split_string(const std::string& s, std::vector<std::string>& v, const std::string& c) {
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
}

inline std::string lower_string(std::string s) {
    int len = s.size();
    for(int i = 0; i < len; ++i){
        if(s[i] >= 'A' && s[i] <= 'Z'){
            s[i] += 32;
        }
    }
    return s;
}

inline std::string upper_string(std::string s) {
    int len = s.size();
    for(int i = 0; i < len; ++i){
        if(s[i] >= 'a' && s[i] <= 'z'){
            s[i] -= 32;
        }
    }
    return s;
}

inline std::string combine_vector_string(const std::vector<std::string>& v) {
    std::string res = "";
    for(int i=0; i<v.size(); ++i) {
        if(i > 0) res += ", ";
        res += v[i];
    }
    return res;
}

inline std::string trim(std::string s) {
    if(!s.empty()) {
        s.erase(0, s.find_first_not_of(" "));
        s.erase(s.find_last_not_of(" ") + 1);
    }
    return s;
}

inline std::string double2string(double num) {
    std::ostringstream myos;
    myos << num;
    std::string result = myos.str();
    return result;
}

#endif
