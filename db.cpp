#include "db.hpp"
#include <fstream>
#include <iostream>
#include <direct.h>
#include <cstring>
#include "global.hpp"

using namespace std;
vector<Column> Table::columns;

Table::Table(const string &name, const vector<Column> &cols)
{
    columns = cols;
    Context::getTableName() = name;
    Context::getFilePath() = "data/" + Context::getTableName() + ".db";
    std::ofstream file(Context::getFilePath(), std::ios::out | std::ios::app);
    if (!file)
        throw std::runtime_error("Failed to create or open file: " + Context::getFilePath());
    file.close();

    saveSchema();
}

Table Table::loadFromSchema(const string &tableName)
{
    ifstream schema("data/" + tableName + ".schema");
    if (!schema)
    {
        throw runtime_error("Schema for table '" + tableName + "' not found.");
    }

    vector<Column> cols;
    string name, type;
    size_t size;
    while (schema >> name >> type >> size)
    {
        cols.push_back({name, type, size});
    }
    return Table(tableName, cols);
}

void Table::saveSchema()
{
    ofstream schema("data/" + Context::getTableName() + ".schema");
    for (const auto &col : columns)
    {
        schema << col.name << " " << col.type << " " << col.size << "\n";
    }
    schema.close();
}

void Table::insert(const vector<string> &values, string filePath)
{
    ofstream file(filePath, ios::binary | ios::app);
    for (size_t i = 0; i < columns.size(); ++i) 
    {
        char buffer[256] = {0};
        strncpy(buffer, values[i].c_str(), columns[i].size);
        file.write(buffer, columns[i].size);
    }
    file.close();
}

void Table::selectAll(string tableName)
{
    ifstream file("data/" + tableName + ".db", ios::binary);
    if (!file)
    {
        cerr << "Error reading data file.\n";
        return;
    }

    size_t recordSize = 0;
    for (auto &col : columns)
        recordSize += col.size;
    vector<char> buffer(recordSize);

    while (file.read(buffer.data(), recordSize))
    {
        size_t offset = 0;
        for (const auto &col : columns)
        {
            string val(buffer.data() + offset, col.size);
            val.erase(val.find('\0'));
            cout << col.name << ": " << val << " ";
            offset += col.size;
        }
        cout << "\n";
    }

    file.close();
}

void Table::selectWhere(string tableName, const string &whereColumn, const string &compareOp, const string &whereValue)
{
    int columnIndex = -1;
    size_t columnOffset = 0;
    
    for (size_t i = 0; i < columns.size(); i++)
    {
        if (columns[i].name == whereColumn)
        {
            columnIndex = i;
            break;
        }
        columnOffset += columns[i].size;
    }
    
    if (columnIndex == -1)
    {
        cerr << "Error: Column '" << whereColumn << "' not found in table '" << tableName << "'.\n";
        return;
    }    
    ifstream file("data/" + tableName + ".db", ios::binary);
    if (!file)
    {
        cerr << "Error reading data file.\n";
        return;
    }
    size_t recordSize = 0;
    for (auto &col : columns)
        recordSize += col.size;
    vector<char> buffer(recordSize);    
    bool foundMatches = false;
    cout << "Records where " << whereColumn << " " << compareOp << " " << whereValue << ":\n";    
    bool isNumericColumn = (columns[columnIndex].type == "INT");
    int numericValue = 0;    
    if (isNumericColumn) {
        try {
            numericValue = stoi(whereValue);
        }
        catch (...) {
            cerr << "Error: Value '" << whereValue << "' cannot be converted to numeric for column '" 
                 << whereColumn << "' of type INT.\n";
            file.close();
            return;
        }
    }    
    while (file.read(buffer.data(), recordSize))
    {
        string fieldValueStr(buffer.data() + columnOffset, columns[columnIndex].size);
        fieldValueStr.erase(fieldValueStr.find('\0'));        
        bool matches = false;
        if (isNumericColumn) {
            int recordValue;
            try {
                recordValue = stoi(fieldValueStr);
            }
            catch (...) {
                continue;
            }            
            if (compareOp == "=") {
                matches = (recordValue == numericValue);
            }
            else if (compareOp == ">") {
                matches = (recordValue > numericValue);
            }
            else if (compareOp == "<") {
                matches = (recordValue < numericValue);
            }
            else if (compareOp == ">=") {
                matches = (recordValue >= numericValue);
            }
            else if (compareOp == "<=") {
                matches = (recordValue <= numericValue);
            }
        }
        else {
            if (compareOp == "=") {
                matches = (fieldValueStr == whereValue);
            }
            else if (compareOp == ">") {
                matches = (fieldValueStr > whereValue);
            }
            else if (compareOp == "<") {
                matches = (fieldValueStr < whereValue);
            }
            else if (compareOp == ">=") {
                matches = (fieldValueStr >= whereValue);
            }
            else if (compareOp == "<=") {
                matches = (fieldValueStr <= whereValue);
            }
        }        
        if (matches) {
            foundMatches = true;
            size_t offset = 0;
            for (const auto &col : columns)
            {
                string val(buffer.data() + offset, col.size);
                val.erase(val.find('\0'));
                cout << col.name << ": " << val << " ";
                offset += col.size;
            }
            cout << "\n";
        }
    }
    if (!foundMatches)
        cout << "No records found matching the condition.\n";    
    file.close();
}

string Table::getTableName()
{
    return Context::getTableName();
}

void Table::update(const string &colToUpdate, const string &newVal, const string &whereCol, const string &whereVal, string filePath)
{
    ifstream in(filePath, ios::binary);
    if (!in)
    {
        cerr << "Failed to open table file for reading.\n";
        return;
    }

    size_t rowSize = 0;
    for (auto &col : columns)
        rowSize += col.size;

    vector<vector<string>> allRows;
    vector<char> buffer(rowSize);

    while (in.read(buffer.data(), rowSize))
    {
        vector<string> row;
        size_t offset = 0;
        for (auto &col : columns)
        {
            string val(buffer.data() + offset, col.size);
            val = val.substr(0, val.find('\0'));
            row.push_back(val);
            offset += col.size;
        }

        if (matchCondition(columns, row, whereCol, whereVal))
        {
            for (size_t i = 0; i < columns.size(); ++i)
            {
                if (columns[i].name == colToUpdate)
                {
                    row[i] = newVal;
                    break;
                }
            }
        }

        allRows.push_back(row);
    }

    for (auto row : allRows)
    {
        for (auto col : row)
        {
            cout << col << " ";
        }
        cout << endl;
    }

    in.close();

    ofstream out(filePath, ios::binary | ios::trunc);
    for (auto &row : allRows)
    {
        for (size_t i = 0; i < columns.size(); ++i)
        {
            string val = row[i];
            val.resize(columns[i].size, '\0');
            out.write(val.c_str(), columns[i].size);
        }
    }
    std::cout << "Updated rows in: " << filePath << "\n";
    out.close();
}
void Table::deleteWhere(const string &colName, const string &value, string filePath)
{
    ifstream in(filePath, ios::binary);
    if (!in)
    {
        cerr << "Failed to open table file.\n";
        return;
    }

    size_t rowSize = 0;
    for (auto &col : columns)
        rowSize += col.size;

    vector<vector<string>> remainingRows;
    vector<char> buffer(rowSize);

    while (in.read(buffer.data(), rowSize))
    {
        vector<string> row;
        size_t offset = 0;
        for (auto &col : columns)
        {
            string val(buffer.data() + offset, col.size);
            val = val.substr(0, val.find('\0'));
            row.push_back(val);
            offset += col.size;
        }

        if (!matchCondition(columns, row, colName, value))
        {
            remainingRows.push_back(row);
        }
    }

    in.close();

    ofstream out(filePath, ios::binary | ios::trunc);
    for (auto &row : remainingRows)
    {
        for (size_t i = 0; i < columns.size(); ++i)
        {
            string val = row[i];
            val.resize(columns[i].size, '\0');
            out.write(val.c_str(), columns[i].size);
        }
    }

    out.close();
    std::cout << "Inserted row into: " << filePath << "\n";
}

bool matchCondition(const vector<Column> &columns, const vector<string> &row, const string &colName, const string &value)
{
    for (size_t i = 0; i < columns.size(); i++)
    {
        if (columns[i].name == colName && row[i] == value)
        {
            return true;
        }
    }
    return false;
}

