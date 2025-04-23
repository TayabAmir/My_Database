#pragma once
#include "B-trees.hpp"
#include <string>
#include <vector>
#include <unordered_map>

using namespace std;

struct Column
{
    string name;
    string type;
    size_t size;
};

class Table
{

public:
    static vector<Column> columns;
    Table(const string &name, const vector<Column> &cols);
    static Table loadFromSchema(const string &tableName);
    static void insert(const vector<string> &values, string filePath);
    static vector<vector<string>> selectAll(string tableName);
    static void saveSchema();
    static string getTableName();
    void selectWhere(string tableName, const string &whereColumn, const string &whereValue)
    {
        selectWhere(tableName, whereColumn, "=", whereValue);
    }
    void selectWhere(string tableName, const string &whereColumn, const string &compareOp, const string &whereValue);
    static void update(const string &colToUpdate, const string &newVal,
                       const string &whereCol, const string &compareOp,
                       const string &whereVal, const string &filePath);
    static void deleteWhere(const string &colName, const string &value, string filePath);
    void selectWhereWithExpression(const string &tableName, const string &whereClause);

private:
    bool matchCond(const string &lhs, const string &rhs, const string &compareOp);
    bool evaluateCondition(const string &expr, const vector<string> &row);
    vector<string> tokenize(const string &expr);
    bool evalLogic(string expr);
    vector<string> infixToPostfix(const string &infix);
    bool evaluatePostfix(const vector<string> &tokens);
    int precedence(const string &op);
    string replaceValues(const string &expr, const vector<string> &row);
};
bool matchCondition(const vector<Column> &columns, const vector<string> &row, const string &colName, const string &value);