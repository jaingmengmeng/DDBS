#include "TableData.h"

TableData::TableData(std::vector<std::string> anames) :
anames(anames) {}

TableData::TableData(std::vector<std::string> anames, std::vector<std::vector<std::string>> datas) :
anames(anames), datas(datas) {}
