#pragma once
#include <string>
#include "Transaction.hpp"

class Context
{
public:
    static std::string tableName;
    static std::string filePath;
    static Transaction transaction;

    static std::string &getFilePath();
    static Transaction &getTransaction();
    static std::string &getTableName();
};
