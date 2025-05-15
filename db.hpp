#ifndef DB_HPP
#define DB_HPP

#include <string>
#include <vector>
#include <map>
#include "global.hpp"

using namespace std;

class BPlusTree;

struct Column
{
    string name;
    string type;
    size_t size;
    bool isPrimaryKey = false;
    bool isForeignKey = false;
    string refTable;
    string refColumn;
    bool isUnique = false;
    bool isNotNull = false;
    bool isIndexed = false;
};

class Table
{
private:
    map<string, BPlusTree *> indexes;

    void saveSchema();
    bool validateWhereClauseColumns(const string &whereClause);
    void rebuildIndex(const string &colName);
    void saveIndex(const string &colName);
    vector<string> tokenize(const string &expr);
    int precedence(const string &op);
    bool evalLogic(string expr);
    bool checkForeignKeyReferences(const string &pkColumn, const string &pkValue, string &errorTable, string &errorColumn);
    vector<string> infixToPostfix(const string &infix);
    bool evaluatePostfix(const vector<string> &tokens);
    bool matchCond(const string &lhs, const string &rhs, const string &compareOp);
    string replaceValues(const string &expr, const vector<string> &row, const vector<Column> &columns);
    size_t getRowSize() const;

public:
    vector<Column> columns;
    string filePath;
    string schemaPath;
    string tableName;
    Table(const string &name, const vector<Column> &cols, const string &dbName = Context::getInstance().getCurrentDatabase());
    ~Table();
    static Table loadFromSchema(const string &tableName, const string &dbName = Context::getInstance().getCurrentDatabase());
    bool createIndex(const string &colName);
    void loadIndex(const string &colName);
    bool insert(const vector<string> &values, string filePath);
    vector<vector<string>> selectAll(string tableName) const;
    bool selectWhere(string tableName, const string &whereColumn, const string &compareOp, const string &whereValue);
    bool selectWhereWithExpression(const string &tableName, const string &whereClause);
    bool selectJoin(const string &table1Name, const Table &table2, const string &table2Name, const string &joinCondition);
    string getTableName();
    bool update(const string &colToUpdate, const string &newVal, const string &whereClause, const string &filePath);
    bool deleteWhere(const string &conditionExpr, const string &filePath);
    bool evaluateCondition(const string &expr, const vector<string> &row);
    vector<Column> getColumns() const { return columns; }
};

#endif