#pragma once
#include <string>
#include <vector>
#include <map>
#include <memory>

enum class Operation
{
    INSERT,
    UPDATE,
    DELETE
};

struct LogEntry
{
    Operation op;
    std::string columnName;
    std::string tableName;
    std::vector<std::string> newValues;
    std::string whereClause;
    std::string checkpointId;
};

class Transaction
{
private:
    std::vector<LogEntry> log;
    std::map<std::string, size_t> checkpoints;
    std::string currentCheckpoint;
    std::vector<std::string> prepareTemporaryFiles(const std::vector<LogEntry> &entries);
    bool applyOperations(const std::vector<LogEntry> &entries, const std::vector<std::string> &tempFiles);
    bool finalizeCommit(const std::vector<std::string> &affectedTables, const std::vector<std::string> &tempFiles);
    void cleanupTemporaryFiles(const std::vector<std::string> &tempFiles);

public:
    bool inTransaction = false;
    bool isDatabaseGiven = false;
    void begin();
    void commit();
    void rollback();
    void createCheckpoint(const std::string &checkpointId);
    void rollbackToCheckpoint(const std::string &checkpointId);
    void commitToCheckpoint(const std::string &checkpointId);
    bool hasCheckpoint(const std::string &checkpointId) const;
    std::vector<std::string> listCheckpoints() const;
    bool isActive() const { return inTransaction; }
    std::string getCurrentCheckpoint() const { return currentCheckpoint; }
    void addInsertOperation(const std::string &tableName, const std::vector<std::string> &newValues);
    void addUpdateOperation(const std::string &tableName, const std::vector<std::string> &newValues, const std::string &columnToUpdate, const std::string &whereClause);
    void addDeleteOperation(const std::string &tableName, const std::string &whereClause);
};