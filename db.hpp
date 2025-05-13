#ifndef DB_HPP
#define DB_HPP

#include <string>
#include <vector>
#include <map>

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
    void rebuildIndex(const string &colName);
    void saveIndex(const string &colName);
    vector<string> tokenize(const string &expr);
    int precedence(const string &op);
    bool evalLogic(string expr);
    vector<string> infixToPostfix(const string &infix);
    bool evaluatePostfix(const vector<string> &tokens);
    bool matchCond(const string &lhs, const string &rhs, const string &compareOp);
    string replaceValues(const string &expr, const vector<string> &row, const vector<Column> &columns);

public:
    vector<Column> columns;
    string filePath;
    string schemaPath;
    string tableName;
    Table(const string &name, const vector<Column> &cols);
    ~Table();
    static Table loadFromSchema(const string &tableName);
    void createIndex(const string &colName);
    void loadIndex(const string &colName);
    void insert(const vector<string> &values, string filePath);
    vector<vector<string>> selectAll(string tableName);
    void selectMultiJoin(const vector<pair<string, Table>> &tables, const vector<string> &joinConditions);
    void selectWhere(string tableName, const string &whereColumn, const string &compareOp, const string &whereValue);
    void selectWhereWithExpression(const string &tableName, const string &whereClause);
    void selectJoin(const string &table1Name, Table &table2, const string &table2Name, const string &joinCondition);
    string getTableName();
    void update(const string &colToUpdate, const string &newVal, const string &whereClause, const string &filePath);
    void deleteWhere(const string &conditionExpr, const string &filePath);
    bool evaluateCondition(const string &expr, const vector<string> &row);
    vector<Column> getColumns() const { return columns; }
};

#endif