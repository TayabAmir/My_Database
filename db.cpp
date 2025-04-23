#include "db.hpp"
#include <fstream>
#include <iostream>
#include <direct.h>
#include <cstring>
#include "global.hpp"
#include "B-trees.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include <regex>
#include <stack>
#include <iomanip>
#include <functional>

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

vector<vector<string>> Table::selectAll(string tableName)
{
    ifstream file("data/" + tableName + ".db", ios::binary);
    if (!file)
    {
        cerr << "Error reading data file.\n";
        return {}; // Return an empty vector if file can't be opened.
    }

    size_t recordSize = 0;
    for (auto &col : columns)
        recordSize += col.size;

    vector<char> buffer(recordSize);
    vector<vector<string>> result; // Vector to store the rows (each row is a vector of strings)

    while (file.read(buffer.data(), recordSize))
    {
        size_t offset = 0;
        vector<string> row; // This will store the current row's values

        for (const auto &col : columns)
        {
            string val(buffer.data() + offset, col.size);
            val.erase(val.find('\0')); // Remove any trailing null characters
            row.push_back(val);        // Add the value to the current row
            offset += col.size;
        }

        result.push_back(row); // Add the row to the result vector
    }

    file.close();
    return result; // Return the vector of rows
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

void Table::selectWhereWithExpression(const string &tableName, const string &whereClause)
{
    Table table = Table::loadFromSchema(tableName);
    vector<vector<string>> rows = table.selectAll(tableName);

    cout << left; 

    for (const auto &col : table.columns)
    {
        cout << setw(col.size) << col.name << " | ";
    }
    cout << endl;

    for (const auto &col : table.columns)
    {
        cout << string(col.size, '-') << "-+-";
    }
    cout << endl;

    for (const auto &row : rows)
    {
        if (evaluateCondition(whereClause, row))
        {
            for (size_t i = 0; i < row.size(); ++i)
            {
                cout << setw(table.columns[i].size) << row[i] << " | "; 
            }
            cout << endl;
        }
    }
}

bool Table::evaluateCondition(const string &expr, const vector<string> &row)
{
    string processedExpr = replaceValues(expr, row);
    return evalLogic(processedExpr);
}

string Table::replaceValues(const string &expr, const vector<string> &row)
{
    string result = expr;
    for (size_t i = 0; i < columns.size(); ++i)
    {
        string colPattern = "\\b" + columns[i].name + "\\b";
        result = regex_replace(result, regex(colPattern), "\"" + row[i] + "\"");
    }
    return result;
}

vector<string> Table::infixToPostfix(const string &infix)
{
    vector<string> tokens = tokenize(infix);
    vector<string> output;
    stack<string> ops;

    for (const auto &token : tokens)
    {
        if (token == "(")
        {
            ops.push(token);
        }
        else if (token == ")")
        {
            while (!ops.empty() && ops.top() != "(")
            {
                output.push_back(ops.top());
                ops.pop();
            }
            if (!ops.empty())
                ops.pop(); 
        }
        else if (token == "&&" || token == "||" || token == "!")
        {
            while (!ops.empty() && precedence(ops.top()) >= precedence(token))
            {
                output.push_back(ops.top());
                ops.pop();
            }
            ops.push(token);
        }
        else
        {
            output.push_back(token);
        }
    }

    while (!ops.empty())
    {
        output.push_back(ops.top());
        ops.pop();
    }

    return output;
}

bool Table::evaluatePostfix(const vector<string> &tokens)
{
    stack<bool> st;
    int i = 0;

    while (i < tokens.size())
    {
        if (tokens[i] == "&&")
        {
            bool b = st.top();
            st.pop();
            bool a = st.top();
            st.pop();
            st.push(a && b);
            i++;
        }
        else if (tokens[i] == "||")
        {
            bool b = st.top();
            st.pop();
            bool a = st.top();
            st.pop();
            st.push(a || b);
            i++;
        }
        else if (tokens[i] == "!")
        {
            bool a = st.top();
            st.pop();
            st.push(!a);
            i++;
        }
        else
        {
            string lhs = tokens[i];
            string op = tokens[i + 1];
            string rhs = tokens[i + 2];
            st.push(matchCond(lhs, rhs, op));
            i += 3;
        }
    }

    return !st.empty() && st.top();
}

bool Table::matchCond(const string &lhs, const string &rhs, const string &compareOp)
{
    if (compareOp == "=")
        return lhs == rhs;
    if (compareOp == "!=")
        return lhs != rhs;
    if (compareOp == ">")
        return lhs > rhs;
    if (compareOp == "<")
        return lhs < rhs;
    if (compareOp == ">=")
        return lhs >= rhs;
    if (compareOp == "<=")
        return lhs <= rhs;
    return false;
}

vector<string> Table::tokenize(const string &expr)
{
    vector<string> tokens;
    string token;
    istringstream iss(expr);

    while (iss >> token)
    {
        if ((token.front() == '"' && token.back() == '"') ||
            (token.front() == '\'' && token.back() == '\''))
        {
            token = token.substr(1, token.size() - 2);
        }

        tokens.push_back(token);
    }

    return tokens;
}

int Table::precedence(const string &op)
{
    if (op == "!")
        return 3;
    if (op == "&&")
        return 2;
    if (op == "||")
        return 1;
    return 0;
}

bool Table::evalLogic(string expr)
{
    expr = regex_replace(expr, regex(R"(\bAND\b)", regex::icase), "&&");
    expr = regex_replace(expr, regex(R"(\bOR\b)", regex::icase), "||");
    expr = regex_replace(expr, regex(R"(\bNOT\b)", regex::icase), "!");

    vector<string> postfix = infixToPostfix(expr);
    return evaluatePostfix(postfix);
}
