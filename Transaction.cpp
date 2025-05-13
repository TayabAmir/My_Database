#include "transaction.hpp"
#include "db.hpp"
#include "global.hpp"
#include <iostream>
#include <fstream>
#include <stdexcept>
#include <unordered_set>
#include <cstdio>

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
        std::string tableName = entry.tableName;
        std::string sourceFile = "data/" + tableName + ".db";
        std::string tempFile = "data/" + tableName + ".db.temp";

        if (preparedTables.count(tableName) == 0)
        {
            std::ifstream src(sourceFile, std::ios::binary);
            std::ofstream dst(tempFile, std::ios::binary);
            dst << src.rdbuf();
            preparedTables.insert(tableName);
        }
    }

    for (const auto &entry : log)
    {
        std::string tempFile = "data/" + entry.tableName + ".db.temp";
        Table table = Table::loadFromSchema(entry.tableName);

        switch (entry.op)
        {
        case Operation::INSERT:
            table.insert(entry.newValues, tempFile);
            std::cout << "Inserted into " << entry.tableName << "\n";
            break;

        case Operation::UPDATE:
            table.update(entry.columnName, entry.newValues[0], entry.whereClause, tempFile);
            std::cout << "Updated " << entry.tableName << " where " << entry.whereClause << "\n";
            break;

        case Operation::DELETE:
            table.deleteWhere(entry.whereClause, tempFile);
            std::cout << "Deleted from " << entry.tableName << " where " << entry.whereClause << "\n";
            break;

        default:
            std::cerr << "Unknown operation.\n";
            break;
        }
    }

    // Step 3: Replace original files with temp files
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
            perror("Rename Error");
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
        std::cerr << "No active transaction to log INSERT.\n";
        return;
    }

    LogEntry entry = {
        Operation::INSERT,
        "", // No column name for INSERT
        tableName,
        newValues,
        "" // No WHERE clause for INSERT
    };
    log.push_back(entry);
    std::cout << "Logged INSERT on table " << tableName << "\n";
}

void Transaction::addUpdateOperation(const std::string &tableName,
                                     const std::vector<std::string> &newValues,
                                     const std::string &columnToUpdate,
                                     const std::string &whereClause)
{
    if (!inTransaction)
    {
        std::cerr << "No active transaction to log UPDATE.\n";
        return;
    }

    LogEntry entry = {
        Operation::UPDATE,
        columnToUpdate,
        tableName,
        newValues,
        whereClause};
    log.push_back(entry);
    std::cout << "Logged UPDATE on " << tableName << " with WHERE: " << whereClause << "\n";
}

void Transaction::addDeleteOperation(const std::string &tableName, const std::string &whereClause)
{
    if (!inTransaction)
    {
        std::cerr << "No active transaction to log DELETE.\n";
        return;
    }

    LogEntry entry = {
        Operation::DELETE,
        "", // No column for DELETE
        tableName,
        {}, // No new values for DELETE
        whereClause};
    log.push_back(entry);
    std::cout << "Logged DELETE on " << tableName << " with WHERE: " << whereClause << "\n";
}
