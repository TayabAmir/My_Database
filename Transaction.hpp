#pragma once
#include <string>
#include <vector>

enum class Operation
{
    INSERT,
    UPDATE,
    DELETE
};

struct LogEntry
{
    Operation op;
    std::string columnName; // only for UPDATE
    std::string tableName;
    std::vector<std::string> newValues; // for INSERT/UPDATE
    std::string whereClause;            // full condition for DELETE/UPDATE
};

class Transaction
{
private:
    std::vector<LogEntry> log;

public:
    bool inTransaction = false;
    bool isDatabaseGiven = false;
    void begin();
    void commit();
    void rollback();

    bool isActive() const { return inTransaction; }

    void addInsertOperation(const std::string &tableName, const std::vector<std::string> &newValues);
    void addUpdateOperation(const std::string &tableName,
                            const std::vector<std::string> &newValues,
                            const std::string &columnToUpdate,
                            const std::string &whereClause);
    void addDeleteOperation(const std::string &tableName, const std::string &whereClause);
};
