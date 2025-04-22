#pragma once
#include <string>
#include <vector>
#include <tuple>

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
    std::vector<std::string> oldValues;
    std::string conditionColumn;
    std::string conditionValue;
    std::string compareOp;
};

class Transaction
{
private:
    std::vector<LogEntry> log;

public:
    bool inTransaction = false;

    void begin();
    void commit();
    void rollback();

    bool isActive() const { return inTransaction; }

    void addInsertOperation(const std::string &tableName, const std::vector<std::string> &newValues);
    void addUpdateOperation(const std::string &tableName,
        const std::vector<std::string> &oldValues,
        const std::vector<std::string> &newValues,
        const std::string &conditionColumn,
        const std::string &conditionValue,
        const std::string &columnToUpdate,
        const std::string &compareOp);
    void addDeleteOperation(const std::string &tableName,
                            const std::vector<std::string> &oldValues,
                            const std::string &conditionColumn,
                            const std::string &conditionValue);
};
