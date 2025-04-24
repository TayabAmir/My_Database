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
    static vector<Column> columns;

public:
    Table(const string &name, const vector<Column> &cols);
    static Table loadFromSchema(const string &tableName);
    static void insert(const vector<string> &values, string filePath);
    static void selectAll(string tableName);
    static void saveSchema();
    static string getTableName();
    void selectWhere(string tableName, const string &whereColumn, const string &whereValue) {
        selectWhere(tableName, whereColumn, "=", whereValue);
    }    
    void selectWhere(string tableName, const string &whereColumn, const string &compareOp, const string &whereValue);
    static void selectWhereComplex(string tableName, 
                                 const vector<string> &whereColumns,
                                 const vector<string> &compareOps,
                                 const vector<string> &whereValues,
                                 const vector<string> &logicalOps);
    static void update(const string &colToUpdate, const string &newVal,
        const string &whereCol, const string &compareOp,
        const string &whereVal, const string &filePath);
    static void deleteWhere(const string &colName, const string &value, string filePath);
};

bool matchCondition(const vector<Column> &columns, const vector<string> &row, const string &colName, const string &value);

