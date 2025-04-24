#pragma once
#include "B-trees.hpp"
#include <string>
#include <vector>
#include <unordered_map>

using namespace std;

struct Column {
    string name;
    string type;
    size_t size;
    bool isPrimaryKey = false;
    bool isForeignKey = false;
    string refTable;
    string refColumn;
    bool isUnique = false;
    bool isNotNull = false;

};


class Table
{

public:
    vector<Column> columns;
    string schemaPath;
    string filePath;
    string tableName;
    Table(const string &name, const vector<Column> &cols);
    static Table loadFromSchema(const string &tableName);
    void insert(const vector<string> &values, string filePath);
    vector<vector<string>> selectAll(string tableName);
    void saveSchema();
    string getTableName();
    void selectWhere(string tableName, const string &whereColumn, const string &whereValue)
    {
        selectWhere(tableName, whereColumn, "=", whereValue);
    }
    void selectWhere(string tableName, const string &whereColumn, const string &compareOp, const string &whereValue);
    void update(const string &colToUpdate, const string &newVal,
                       const string &whereClause, const string &filePath);
    void deleteWhere(const string &whereClause, const string &filePath);
    void selectWhereWithExpression(const string &tableName, const string &whereClause);

private:
    static bool matchCond(const string &lhs, const string &rhs, const string &compareOp);
    bool evaluateCondition(const string &expr, const vector<string> &row);
    static vector<string> tokenize(const string &expr);
    static bool evalLogic(string expr);
    static vector<string> infixToPostfix(const string &infix);
    static bool evaluatePostfix(const vector<string> &tokens);
    static int precedence(const string &op);
    static string replaceValues(const string &expr, const vector<string> &row, const vector<Column> &columns);
};
bool matchCondition(const vector<Column> &columns, const vector<string> &row, const string &colName, const string &value);