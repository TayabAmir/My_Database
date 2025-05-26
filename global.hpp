#ifndef GLOBAL_HPP
#define GLOBAL_HPP

#include <string>
#include "Transaction.hpp"
#include "acl.hpp"
using namespace std;

class Context
{
private:
    static Context *instance;
    Transaction transaction;
    string currentDatabase;

    Context() : currentDatabase("default") {}

public:
    static Context &getInstance()
    {
        if (!instance)
            instance = new Context();
        return *instance;
    }

    Transaction &getTransaction()
    {
        return transaction;
    }

    void setCurrentDatabase(const string &dbName)
    {
        currentDatabase = dbName;
    }

    string getCurrentDatabase() const
    {
        return currentDatabase;
    }

    string getDatabasePath() const
    {
        return "databases/" + currentDatabase + "/";
    }

    ACL &getACL()
    {
        return ACL::getInstance();
    }
};

#endif