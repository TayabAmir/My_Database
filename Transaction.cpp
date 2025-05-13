#include "transaction.hpp"
#include "db.hpp"
#include "global.hpp"
#include <iostream>
#include <fstream>
#include <stdexcept>
#include <unordered_set>
#include <cstdio>
#include <filesystem>

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
    std::string dbPath = Context::getInstance().getDatabasePath();
    std::vector<std::string> tempFiles;

    // Step 1: Prepare temporary files
    try
    {
        for (const auto &entry : log)
        {
            std::string tableName = entry.tableName;
            std::string sourceFile = dbPath + tableName + ".db";
            std::string tempFile = dbPath + tableName + ".db.temp";

            if (preparedTables.count(tableName) == 0)
            {
                std::ifstream src(sourceFile, std::ios::binary);
                if (!src)
                {
                    throw std::runtime_error("Failed to open source file: " + sourceFile);
                }
                std::ofstream dst(tempFile, std::ios::binary);
                if (!dst)
                {
                    throw std::runtime_error("Failed to create temp file: " + tempFile);
                }
                dst << src.rdbuf();
                src.close();
                dst.close();
                preparedTables.insert(tableName);
                tempFiles.push_back(tempFile);
            }
        }

        // Step 2: Apply operations to temporary files
        for (const auto &entry : log)
        {
            std::string tempFile = dbPath + entry.tableName + ".db.temp";
            Table table = Table::loadFromSchema(entry.tableName, Context::getInstance().getCurrentDatabase());

            switch (entry.op)
            {
            case Operation::INSERT:
                table.insert(entry.newValues, tempFile);
                std::cout << "Inserted into " << entry.tableName << " in database " << Context::getInstance().getCurrentDatabase() << "\n";
                break;

            case Operation::UPDATE:
                table.update(entry.columnName, entry.newValues[0], entry.whereClause, tempFile);
                std::cout << "Updated " << entry.tableName << " where " << entry.whereClause << " in database " << Context::getInstance().getCurrentDatabase() << "\n";
                break;

            case Operation::DELETE:
                table.deleteWhere(entry.whereClause, tempFile);
                std::cout << "Deleted from " << entry.tableName << " where " << entry.whereClause << " in database " << Context::getInstance().getCurrentDatabase() << "\n";
                break;

            default:
                throw std::runtime_error("Unknown operation in transaction log");
            }
        }

        // Step 3: Replace original files with temporary files
        for (const auto &tableName : preparedTables)
        {
            std::string tempFile = dbPath + tableName + ".db.temp";
            std::string finalFile = dbPath + tableName + ".db";

            if (std::remove(finalFile.c_str()) != 0 && errno != ENOENT)
            {
                cout << "Failed to remove original file" << "\n ";
                return;
            }

            if (std::rename(tempFile.c_str(), finalFile.c_str()) != 0)
            {
                cout << "Failed to rename temp file to original file" << "\n ";
                return;
            }
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Commit failed: " << e.what() << "\n";
        // Clean up temporary files
        for (const auto &tempFile : tempFiles)
        {
            std::remove(tempFile.c_str());
        }
        return;
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
        "",
        tableName,
        newValues,
        ""};
    log.push_back(entry);
    std::cout << "Logged INSERT on table " << tableName << " in database " << Context::getInstance().getCurrentDatabase() << "\n";
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
    std::cout << "Logged UPDATE on " << tableName << " with WHERE: " << whereClause << " in database " << Context::getInstance().getCurrentDatabase() << "\n";
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
        "",
        tableName,
        {},
        whereClause};
    log.push_back(entry);
    std::cout << "Logged DELETE on " << tableName << " with WHERE: " << whereClause << " in database " << Context::getInstance().getCurrentDatabase() << "\n";
}