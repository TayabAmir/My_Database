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
    static void selectWhereComplex(string tableName, 
                                 const vector<string> &whereColumns,
                                 const vector<string> &compareOps,
                                 const vector<string> &whereValues,
                                 const vector<string> &logicalOps);
    static void update(const string &colToUpdate, const string &newVal,
                       const string &whereClause, const string &filePath);
    static void deleteWhere(const string &whereClause, const string &filePath);
    void selectWhereWithExpression(const string &tableName, const string &whereClause);

private:
    static bool matchCond(const string &lhs, const string &rhs, const string &compareOp);
    static bool evaluateCondition(const string &expr, const vector<string> &row);
    static vector<string> tokenize(const string &expr);
    static bool evalLogic(string expr);
    static vector<string> infixToPostfix(const string &infix);
    static bool evaluatePostfix(const vector<string> &tokens);
    static int precedence(const string &op);
    static string replaceValues(const string &expr, const vector<string> &row);
};
bool matchCondition(const vector<Column> &columns, const vector<string> &row, const string &colName, const string &value);