#include "db.hpp"
#include "transaction.hpp"
#include <iostream>
#include <fstream>
#include <stdexcept>
#include "global.hpp"
#include <cstring>
#include <unordered_set>

void Transaction::begin()
{
    if (inTransaction)
    {
        std::cerr << "A transaction is already in progress.\n";
        return;
    }
    log.clear();
    inTransaction = true;
    std::cout << "Transaction started.\n";
}

void Transaction::commit()
{
    if (!inTransaction)
    {
        std::cerr << "No transaction to commit.\n";
        return;
    }
    std::unordered_set<std::string> preparedTables;

    for (const auto &entry : log)
    {
        std::string sourceFile = "data/" + entry.tableName + ".db";
        std::string tempFile = "data/" + entry.tableName + ".db.temp";
        if (preparedTables.find(entry.tableName) == preparedTables.end())
        {
            std::ifstream src(sourceFile, std::ios::binary);
            std::ofstream dst(tempFile, std::ios::binary);
            dst << src.rdbuf();
            preparedTables.insert(entry.tableName);
        }
    }
    for (const auto &entry : log)
    {
        switch (entry.op)
        {
        case Operation::INSERT:
        {
            Table::insert(entry.newValues, "data/" + entry.tableName + ".db.temp");
            std::cout << "Inserted data into " << entry.tableName << "\n";
        }
        break;
        case Operation::UPDATE:
        {
            Table::update(entry.columnName, entry.newValues[0], entry.conditionColumn, entry.conditionValue, entry.compareOp, "data/" + entry.tableName + ".db.temp");
        }
        break;
        case Operation::DELETE:
        {
            Table::deleteWhere(entry.conditionColumn, entry.conditionValue, "data/" + entry.tableName + ".db.temp");
            std::cout << "Deleted data from " << entry.tableName << "\n";
        }
        break;
        default:
            std::cerr << "Unknown operation.\n";
            break;
        }
    }

    for (const auto &tableName : preparedTables)
    {
        std::string tempFile = "data/" + tableName + ".db.temp";
        std::string finalFile = "data/" + tableName + ".db";
        if (std::remove(finalFile.c_str()) != 0)
        {
            perror(("Failed to remove " + finalFile).c_str());
        }
        if (std::rename(tempFile.c_str(), finalFile.c_str()) != 0)
        {
            std::cerr << "Failed to rename " << tempFile << " to " << finalFile << "\n";
            perror("Error");
        }
    }
    inTransaction = false;
    std::cout << "Transaction committed.\n";
}

void Transaction::rollback()
{
    if (!inTransaction)
    {
        std::cerr << "No transaction to rollback.\n";
        return;
    }
    log.clear();
    inTransaction = false;
    std::cout << "Transaction rolled back.\n";
}

void Transaction::addInsertOperation(const std::string &tableName, const std::vector<std::string> &newValues)
{
    if (!inTransaction)
    {
        std::cerr << "No active transaction to log the operation.\n";
        return;
    }
    LogEntry entry = {Operation::INSERT, "", tableName, newValues, {}, "", ""};
    log.push_back(entry);
    std::cout << "Logged INSERT operation for table " << tableName << "\n";
}

void Transaction::addUpdateOperation(const std::string &tableName,
                                     const std::vector<std::string> &oldValues,
                                     const std::vector<std::string> &newValues,
                                     const std::string &conditionColumn,
                                     const std::string &conditionValue,
                                     const std::string &columnToUpdate,
                                     const std::string &compareOp)
{
    if (!inTransaction)
    {
        std::cerr << "No active transaction to log the operation.\n";
        return;
    }

    LogEntry entry = {
        Operation::UPDATE,
        columnToUpdate,
        tableName,
        newValues,
        oldValues,
        conditionColumn,
        conditionValue,
        compareOp // assign operator
    };

    log.push_back(entry);
    std::cout << "Logged UPDATE operation for table " << tableName << " with operator " << compareOp << "\n";
}

void Transaction::addDeleteOperation(const std::string &tableName,
                                     const std::vector<std::string> &oldValues,
                                     const std::string &conditionColumn,
                                     const std::string &conditionValue)
{
    if (!inTransaction)
    {
        std::cerr << "No active transaction to log the operation.\n";
        return;
    }
    LogEntry entry = {Operation::DELETE, "", tableName, {}, oldValues, conditionColumn, conditionValue};
    log.push_back(entry);
    std::cout << "Logged DELETE operation for table " << tableName << "\n";
}
