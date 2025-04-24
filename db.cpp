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
    if (isNumericColumn)
    {
        try
        {
            numericValue = stoi(whereValue);
        }
        catch (...)
        {
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
        if (isNumericColumn)
        {
            int recordValue;
            try
            {
                recordValue = stoi(fieldValueStr);
            }
            catch (...)
            {
                continue;
            }
            if (compareOp == "=")
            {
                matches = (recordValue == numericValue);
            }
            else if (compareOp == "!=")
            {
                matches = (recordValue != numericValue);
            }
            else if (compareOp == ">")
            {
                matches = (recordValue > numericValue);
            }
            else if (compareOp == "<")
            {
                matches = (recordValue < numericValue);
            }
            else if (compareOp == ">=")
            {
                matches = (recordValue >= numericValue);
            }
            else if (compareOp == "<=")
            {
                matches = (recordValue <= numericValue);
            }
        }
        else
        {
            if (compareOp == "=")
            {
                matches = (fieldValueStr == whereValue);
            }
            else if (compareOp == "!=")
            {
                matches = (fieldValueStr != whereValue);
            }
            else if (compareOp == ">")
            {
                matches = (fieldValueStr > whereValue);
            }
            else if (compareOp == "<")
            {
                matches = (fieldValueStr < whereValue);
            }
            else if (compareOp == ">=")
            {
                matches = (fieldValueStr >= whereValue);
            }
            else if (compareOp == "<=")
            {
                matches = (fieldValueStr <= whereValue);
            }
        }
        if (matches)
        {
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

void Table::selectWhereComplex(string tableName, 
                              const vector<string> &whereColumns,
                              const vector<string> &compareOps,
                              const vector<string> &whereValues,
                              const vector<string> &logicalOps)
{
    if (whereColumns.size() != compareOps.size() || 
        whereColumns.size() != whereValues.size() ||
        whereColumns.size() != logicalOps.size() + 1)
    {
        cerr << "Error: Invalid number of conditions and logical operators.\n";
        return;
    }

    // Validate logical operators
    for (const auto &op : logicalOps)
    {
        if (op != "AND" && op != "OR" && op != "NOT")
        {
            cerr << "Error: Invalid logical operator '" << op << "'. Use AND, OR, or NOT.\n";
            return;
        }
    }

    ifstream file("data/" + tableName + ".db", ios::binary);
    if (!file)
    {
        cerr << "Error reading data file.\n";
        return;
    }

    // Calculate record size
    size_t recordSize = 0;
    for (auto &col : columns)
        recordSize += col.size;

    // Find column indices and offsets
    vector<int> columnIndices;
    vector<size_t> columnOffsets;
    vector<bool> isNumericColumns;
    vector<int> numericValues;

    for (size_t i = 0; i < whereColumns.size(); i++)
    {
        int columnIndex = -1;
        size_t columnOffset = 0;
        for (size_t j = 0; j < columns.size(); j++)
        {
            if (columns[j].name == whereColumns[i])
            {
                columnIndex = j;
                break;
            }
            columnOffset += columns[j].size;
        }
        if (columnIndex == -1)
        {
            cerr << "Error: Column '" << whereColumns[i] << "' not found.\n";
            return;
        }
        columnIndices.push_back(columnIndex);
        columnOffsets.push_back(columnOffset);
        isNumericColumns.push_back(columns[columnIndex].type == "INT");
        
        if (isNumericColumns.back())
        {
            try
            {
                numericValues.push_back(stoi(whereValues[i]));
            }
            catch (...)
            {
                cerr << "Error: Value '" << whereValues[i] << "' cannot be converted to numeric.\n";
                return;
            }
        }
        else
        {
            numericValues.push_back(0); // Placeholder
        }
    }

    vector<char> buffer(recordSize);
    bool foundMatches = false;
    cout << "Records matching complex condition:\n";

    while (file.read(buffer.data(), recordSize))
    {
        bool overallMatch = true;
        vector<bool> conditionResults;

        // First evaluate all conditions
        for (size_t i = 0; i < whereColumns.size(); i++)
        {
            string fieldValueStr(buffer.data() + columnOffsets[i], columns[columnIndices[i]].size);
            fieldValueStr.erase(fieldValueStr.find('\0'));
            
            bool currentMatch = false;
            
            if (isNumericColumns[i])
            {
                int recordValue;
                try
                {
                    recordValue = stoi(fieldValueStr);
                }
                catch (...)
                {
                    currentMatch = false;
                    conditionResults.push_back(currentMatch);
                    continue;
                }

                if (compareOps[i] == "=")
                    currentMatch = (recordValue == numericValues[i]);
                else if (compareOps[i] == "!=")
                    currentMatch = (recordValue != numericValues[i]);
                else if (compareOps[i] == ">")
                    currentMatch = (recordValue > numericValues[i]);
                else if (compareOps[i] == "<")
                    currentMatch = (recordValue < numericValues[i]);
                else if (compareOps[i] == ">=")
                    currentMatch = (recordValue >= numericValues[i]);
                else if (compareOps[i] == "<=")
                    currentMatch = (recordValue <= numericValues[i]);
            }
            else
            {
                if (compareOps[i] == "=")
                    currentMatch = (fieldValueStr == whereValues[i]);
                else if (compareOps[i] == "!=")
                    currentMatch = (fieldValueStr != whereValues[i]);
                else if (compareOps[i] == ">")
                    currentMatch = (fieldValueStr > whereValues[i]);
                else if (compareOps[i] == "<")
                    currentMatch = (fieldValueStr < whereValues[i]);
                else if (compareOps[i] == ">=")
                    currentMatch = (fieldValueStr >= whereValues[i]);
                else if (compareOps[i] == "<=")
                    currentMatch = (fieldValueStr <= whereValues[i]);
            }
            conditionResults.push_back(currentMatch);
        }

        // Then apply logical operators
        overallMatch = conditionResults[0];
        for (size_t i = 1; i < conditionResults.size(); i++)
        {
            if (logicalOps[i-1] == "AND")
                overallMatch = overallMatch && conditionResults[i];
            else if (logicalOps[i-1] == "OR")
                overallMatch = overallMatch || conditionResults[i];
            else if (logicalOps[i-1] == "NOT")
                overallMatch = overallMatch && !conditionResults[i];
        }

        if (overallMatch)
        {
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
        cout << "No records found matching the conditions.\n";
    file.close();
}

string Table::getTableName()
{
    return Context::getTableName();
}

void Table::update(const string &colToUpdate, const string &newVal,
                   const string &whereCol, const string &compareOp,
                   const string &whereVal, const string &filePath)
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

    int whereIndex = -1, updateIndex = -1;
    size_t whereOffset = 0, updateOffset = 0;
    bool isNumericColumn = false;

    for (size_t i = 0; i < columns.size(); i++)
    {
        if (columns[i].name == whereCol)
        {
            whereIndex = i;
            isNumericColumn = (columns[i].type == "INT");
        }
        if (columns[i].name == colToUpdate)
            updateIndex = i;

        if (whereIndex == -1)
            whereOffset += columns[i].size;
        if (updateIndex == -1)
            updateOffset += columns[i].size;
    }

    if (whereIndex == -1 || updateIndex == -1)
    {
        cerr << "Error: Column not found.\n";
        return;
    }

    int numericValue = 0;
    if (isNumericColumn)
    {
        try
        {
            numericValue = stoi(whereVal);
        }
        catch (...)
        {
            cerr << "Error: Cannot convert WHERE value to INT.\n";
            return;
        }
    }

    vector<vector<string>> allRows;
    vector<char> buffer(rowSize);

    while (in.read(buffer.data(), rowSize))
    {
        vector<string> row;
        size_t offset = 0;
        for (auto &col : columns)
        {
            string val(buffer.data() + offset, col.size);
            val.erase(val.find('\0'));
            row.push_back(val);
            offset += col.size;
        }

        string cellVal = row[whereIndex];
        bool match = false;

        if (isNumericColumn)
        {
            try
            {
                int val = stoi(cellVal);
                if (compareOp == "=")
                    match = (val == numericValue);
                else if (compareOp == ">")
                    match = (val > numericValue);
                else if (compareOp == "<")
                    match = (val < numericValue);
                else if (compareOp == ">=")
                    match = (val >= numericValue);
                else if (compareOp == "<=")
                    match = (val <= numericValue);
            }
            catch (...)
            { /* skip malformed row */
            }
        }
        else
        {
            if (compareOp == "=")
                match = (cellVal == whereVal);
            else if (compareOp == ">")
                match = (cellVal > whereVal);
            else if (compareOp == "<")
                match = (cellVal < whereVal);
            else if (compareOp == ">=")
                match = (cellVal >= whereVal);
            else if (compareOp == "<=")
                match = (cellVal <= whereVal);
        }

        if (match)
            row[updateIndex] = newVal;

        allRows.push_back(row);
    }

    in.close();

    ofstream out(filePath, ios::binary | ios::trunc);
    if (!out)
    {
        cerr << "Failed to open table file for writing.\n";
        return;
    }

    for (const auto &row : allRows)
    {
        for (size_t i = 0; i < columns.size(); ++i)
        {
            string val = row[i];
            val.resize(columns[i].size, '\0');
            out.write(val.c_str(), columns[i].size);
        }
    }

    cout << "Updated records successfully.\n";
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
