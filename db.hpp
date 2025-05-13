#ifndef DB_HPP
#define DB_HPP
#include <string>
#include <vector>
#include <map>

struct Column
{
    std::string name;
    std::string type;
    size_t size;
    bool isPrimaryKey = false;
    bool isForeignKey = false;
    bool isUnique = false;
    bool isNotNull = false;
    bool isIndexed = false;
    std::string refTable;
    std::string refColumn;
};

class BPlusTree; // Forward declaration

class Table
{
private:
    std::map<std::string, BPlusTree *> indexes;

    std::vector<std::string> tokenize(const std::string &expr);
    std::vector<std::string> infixToPostfix(const std::string &infix);
    bool evaluatePostfix(const std::vector<std::string> &tokens);
    bool matchCond(const std::string &lhs, const std::string &rhs, const std::string &compareOp);
    int precedence(const std::string &op);
    bool evalLogic(std::string expr);
    std::string replaceValues(const std::string &expr, const std::vector<std::string> &row, const std::vector<Column> &columns);

public:
    std::vector<Column> columns;
    std::string filePath;
    std::string schemaPath;
    std::string tableName;
    Table(const std::string &name, const std::vector<Column> &cols);
    ~Table();
    static Table loadFromSchema(const std::string &tableName);
    void saveSchema();
    void insert(const std::vector<std::string> &values, std::string filePath);
    std::vector<std::vector<std::string>> selectAll(std::string tableName);
    void selectWhere(std::string tableName, const std::string &whereColumn, const std::string &compareOp, const std::string &whereValue);
    void selectWhereWithExpression(const std::string &tableName, const std::string &whereClause);
    void update(const std::string &colToUpdate, const std::string &newVal, const std::string &whereClause, const std::string &filePath);
    void deleteWhere(const std::string &conditionExpr, const std::string &filePath);
    bool evaluateCondition(const std::string &expr, const std::vector<std::string> &row);
    std::string getTableName();
    void createIndex(const std::string &colName);
    void loadIndex(const std::string &colName);
    void saveIndex(const std::string &colName);
    void rebuildIndex(const std::string &colName);
};

#endif