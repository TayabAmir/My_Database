#include <iostream>
#include "global.hpp"

std::string Context::tableName = "";
std::string Context::filePath = "";
Transaction Context::transaction;

std::string &Context::getFilePath()
{
    return filePath;
}

Transaction &Context::getTransaction()
{
    return transaction;
}

std::string &Context::getTableName()
{
    return tableName;
}
