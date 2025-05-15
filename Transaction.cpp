#include "transaction.hpp"
#include "db.hpp"
#include "global.hpp"
#include <iostream>
#include <fstream>
#include <stdexcept>
#include <unordered_set>
#include <cstdio>
#include <filesystem>
#include <chrono>
#include <algorithm>
#include <sstream>

void Transaction::begin()
{
    if (inTransaction)
    {
        std::cerr << "A transaction is already in progress.\n";
        return;
    }
    log.clear();
    checkpoints.clear();
    currentCheckpoint = "";
    inTransaction = true;
    std::cout << "Transaction started.\n";
}

std::vector<std::string> Transaction::prepareTemporaryFiles(const std::vector<LogEntry> &entries)
{
    std::unordered_set<std::string> preparedTables;
    std::string dbPath = Context::getInstance().getDatabasePath() + "data/";
    std::vector<std::string> tempFiles;

    for (const auto &entry : entries)
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

    return tempFiles;
}

bool Transaction::applyOperations(const std::vector<LogEntry> &entries, const std::vector<std::string> &tempFiles)
{
    std::string dbPath = Context::getInstance().getDatabasePath() + "data/";
    std::unordered_set<std::string> preparedTables;

    for (const auto &entry : entries)
    {
        preparedTables.insert(entry.tableName);
    }

    for (const auto &entry : entries)
    {
        std::string tempFile = dbPath + entry.tableName + ".db.temp";
        Table table = Table::loadFromSchema(entry.tableName, Context::getInstance().getCurrentDatabase());

        switch (entry.op)
        {
        case Operation::INSERT:
            if (!table.insert(entry.newValues, tempFile))
            {
                std::cerr << "Failed insertion into " << entry.tableName << " in database "
                          << Context::getInstance().getCurrentDatabase() << "\n";
                return false;
            }
            std::cout << "Applied INSERT into " << entry.tableName
                      << " in database " << Context::getInstance().getCurrentDatabase() << "\n";
            break;

        case Operation::UPDATE:
            if (!table.update(entry.columnName, entry.newValues[0], entry.whereClause, tempFile))
            {
                std::cerr << "Failed update " << entry.tableName << " where " << entry.whereClause
                          << " in database " << Context::getInstance().getCurrentDatabase() << "\n";
                return false;
            }
            std::cout << "Applied UPDATE on " << entry.tableName << " where " << entry.whereClause
                      << " in database " << Context::getInstance().getCurrentDatabase() << "\n";
            break;

        case Operation::DELETE:
            if (!table.deleteWhere(entry.whereClause, tempFile))
            {
                std::cerr << "Failed delete from " << entry.tableName << " where " << entry.whereClause
                          << " in database " << Context::getInstance().getCurrentDatabase() << "\n";
                return false;
            }
            std::cout << "Applied DELETE from " << entry.tableName << " where " << entry.whereClause
                      << " in database " << Context::getInstance().getCurrentDatabase() << "\n";
            break;

        default:
            throw std::runtime_error("Unknown operation in transaction log");
        }
    }

    return true;
}

bool Transaction::finalizeCommit(const std::vector<std::string> &affectedTables, const std::vector<std::string> &tempFiles)
{
    std::string dbPath = Context::getInstance().getDatabasePath() + "data/";

    for (const auto &tableName : affectedTables)
    {
        std::string tempFile = dbPath + tableName + ".db.temp";
        std::string finalFile = dbPath + tableName + ".db";

        // Create backup file
        std::string backupFile = dbPath + tableName + ".db.bak";
        try
        {
            std::filesystem::copy_file(finalFile, backupFile, std::filesystem::copy_options::overwrite_existing);
        }
        catch (const std::exception &e)
        {
            std::cerr << "Warning: Could not create backup: " << e.what() << std::endl;
            // Continue anyway - backup is just a precaution
        }

        if (std::remove(finalFile.c_str()) != 0 && errno != ENOENT)
        {
            std::cerr << "Failed to remove original file: " << finalFile << "\n";
            return false;
        }

        if (std::rename(tempFile.c_str(), finalFile.c_str()) != 0)
        {
            std::cerr << "Failed to rename temp file to original file: " << tempFile << " -> " << finalFile << "\n";
            // Try to restore from backup
            try
            {
                std::filesystem::copy_file(backupFile, finalFile, std::filesystem::copy_options::overwrite_existing);
                std::cerr << "Restored from backup file\n";
            }
            catch (...)
            {
                std::cerr << "Failed to restore from backup!\n";
            }
            return false;
        }

        // Remove backup file
        std::remove(backupFile.c_str());
    }

    return true;
}

void Transaction::cleanupTemporaryFiles(const std::vector<std::string> &tempFiles)
{
    for (const auto &tempFile : tempFiles)
    {
        std::remove(tempFile.c_str());
    }
}

void Transaction::commit()
{
    if (!inTransaction)
    {
        std::cerr << "No transaction to commit.\n";
        return;
    }

    std::unordered_set<std::string> affectedTablesSet;
    std::vector<std::string> tempFiles;

    // Step 1: Prepare temporary files
    try
    {
        tempFiles = prepareTemporaryFiles(log);

        // Collect all affected tables
        for (const auto &entry : log)
        {
            affectedTablesSet.insert(entry.tableName);
        }

        std::vector<std::string> affectedTables(affectedTablesSet.begin(), affectedTablesSet.end());

        // Step 2: Apply all operations
        if (!applyOperations(log, tempFiles))
        {
            std::cerr << "Transaction failed during operation application. Rolling back.\n";
            cleanupTemporaryFiles(tempFiles);
            rollback();
            return;
        }

        // Step 3: Commit changes by replacing original files with temp files
        if (!finalizeCommit(affectedTables, tempFiles))
        {
            std::cerr << "Transaction failed during commit phase. Rolling back.\n";
            cleanupTemporaryFiles(tempFiles);
            rollback();
            return;
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Commit failed: " << e.what() << "\n";
        cleanupTemporaryFiles(tempFiles);
        rollback();
        return;
    }

    // Clear transaction state
    log.clear();
    checkpoints.clear();
    currentCheckpoint = "";
    inTransaction = false;
    isDatabaseGiven = false;
    std::cout << "Transaction committed successfully.\n";
}

void Transaction::rollback()
{
    if (!inTransaction)
    {
        std::cerr << "No transaction to rollback.\n";
        return;
    }
    log.clear();
    checkpoints.clear();
    currentCheckpoint = "";
    inTransaction = false;
    isDatabaseGiven = false;
    std::cout << "Transaction rolled back completely.\n";
}

void Transaction::createCheckpoint(const std::string &checkpointId)
{
    if (!inTransaction)
    {
        std::cerr << "No active transaction for creating checkpoint.\n";
        return;
    }

    if (checkpointId.empty())
    {
        std::cerr << "Checkpoint ID cannot be empty.\n";
        return;
    }

    if (checkpoints.find(checkpointId) != checkpoints.end())
    {
        std::cerr << "Checkpoint '" << checkpointId << "' already exists.\n";
        return;
    }

    // Store the current position in the log
    checkpoints[checkpointId] = log.size();
    currentCheckpoint = checkpointId;

    std::cout << "Created checkpoint '" << checkpointId << "' at position " << log.size() << "\n";
}

void Transaction::rollbackToCheckpoint(const std::string &checkpointId)
{
    if (!inTransaction)
    {
        std::cerr << "No active transaction for rollback to checkpoint.\n";
        return;
    }

    auto it = checkpoints.find(checkpointId);
    if (it == checkpoints.end())
    {
        std::cerr << "Checkpoint '" << checkpointId << "' does not exist.\n";
        return;
    }

    // Remove all log entries after the checkpoint
    size_t position = it->second;
    if (position < log.size())
    {
        log.erase(log.begin() + position, log.end());
    }

    // Remove all checkpoints that come after this one
    std::vector<std::string> checkpointsToRemove;
    for (const auto &cp : checkpoints)
    {
        if (cp.second > position)
        {
            checkpointsToRemove.push_back(cp.first);
        }
    }

    for (const auto &cp : checkpointsToRemove)
    {
        checkpoints.erase(cp);
    }

    currentCheckpoint = checkpointId;
    std::cout << "Rolled back to checkpoint '" << checkpointId << "'\n";
}

void Transaction::commitToCheckpoint(const std::string &checkpointId)
{
    if (!inTransaction)
    {
        std::cerr << "No active transaction for commit to checkpoint.\n";
        return;
    }

    auto it = checkpoints.find(checkpointId);
    if (it == checkpoints.end())
    {
        std::cerr << "Checkpoint '" << checkpointId << "' does not exist.\n";
        return;
    }

    size_t position = it->second;
    std::vector<LogEntry> entriesToCommit;

    // Extract the log entries up to the checkpoint
    for (size_t i = 0; i < position; i++)
    {
        entriesToCommit.push_back(log[i]);
    }

    std::unordered_set<std::string> affectedTablesSet;
    for (const auto &entry : entriesToCommit)
    {
        affectedTablesSet.insert(entry.tableName);
    }
    std::vector<std::string> affectedTables(affectedTablesSet.begin(), affectedTablesSet.end());

    // Execute the commit process for these entries
    std::vector<std::string> tempFiles;

    try
    {
        tempFiles = prepareTemporaryFiles(entriesToCommit);

        if (!applyOperations(entriesToCommit, tempFiles))
        {
            std::cerr << "Checkpoint commit failed during operation application.\n";
            cleanupTemporaryFiles(tempFiles);
            return;
        }

        if (!finalizeCommit(affectedTables, tempFiles))
        {
            std::cerr << "Checkpoint commit failed during finalization.\n";
            cleanupTemporaryFiles(tempFiles);
            return;
        }

        // Remove committed entries from the log
        log.erase(log.begin(), log.begin() + position);

        // Adjust checkpoint positions
        for (auto &cp : checkpoints)
        {
            if (cp.second > position)
            {
                cp.second -= position;
            }
            else
            {
                cp.second = 0;
            }
        }

        std::cout << "Committed to checkpoint '" << checkpointId << "'\n";
    }
    catch (const std::exception &e)
    {
        std::cerr << "Checkpoint commit failed: " << e.what() << "\n";
        cleanupTemporaryFiles(tempFiles);
    }
}

bool Transaction::hasCheckpoint(const std::string &checkpointId) const
{
    return checkpoints.find(checkpointId) != checkpoints.end();
}

std::vector<std::string> Transaction::listCheckpoints() const
{
    std::vector<std::string> result;
    for (const auto &cp : checkpoints)
    {
        result.push_back(cp.first);
    }

    return result;
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
        "",
        currentCheckpoint};
    log.push_back(entry);
    std::cout << "Logged INSERT on table " << tableName << " in database "
              << Context::getInstance().getCurrentDatabase();

    if (!currentCheckpoint.empty())
    {
        std::cout << " (checkpoint: " << currentCheckpoint << ")";
    }
    std::cout << "\n";
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
        whereClause,
        currentCheckpoint};
    log.push_back(entry);
    std::cout << "Logged UPDATE on " << tableName << " with WHERE: " << whereClause
              << " in database " << Context::getInstance().getCurrentDatabase();

    if (!currentCheckpoint.empty())
    {
        std::cout << " (checkpoint: " << currentCheckpoint << ")";
    }
    std::cout << "\n";
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
        whereClause,
        currentCheckpoint};
    log.push_back(entry);
    std::cout << "Logged DELETE on " << tableName << " with WHERE: " << whereClause
              << " in database " << Context::getInstance().getCurrentDatabase();

    if (!currentCheckpoint.empty())
    {
        std::cout << " (checkpoint: " << currentCheckpoint << ")";
    }
    std::cout << "\n";
}