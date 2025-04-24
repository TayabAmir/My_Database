#include "parser.hpp"
#include <sstream>
#include <iostream>
#include <regex>
#include "Transaction.hpp"
#include "global.hpp"

using namespace std;

void handleCreate(const string &query)
{
    regex pattern(R"(CREATE TABLE (\w+)\s*\((.+)\);?)", regex::icase);
    smatch match;
    if (!regex_match(query, match, pattern))
    {
        cout << "Invalid CREATE TABLE syntax.\n";
        return;
    }
    string tableName = match[1];
    string columnsRaw = match[2];
    vector<Column> columns;
    stringstream ss(columnsRaw);
    string col;
    while (getline(ss, col, ','))
    {
        stringstream colStream(col);
        string name, typeFull;
        colStream >> name >> typeFull;
        Column column;
        column.name = name;
        smatch strMatch;
        if (regex_match(typeFull, strMatch, regex(R"(STRING\((\d+)\))", regex::icase)))
        {
            column.type = "STRING";
            column.size = stoi(strMatch[1]);
        }
        else if (typeFull == "INT")
        {
            column.type = "INT";
            column.size = 4;
        }
        else
        {
            cout << "Unknown type: " << typeFull << "\n";
            return;
        }

        columns.push_back(column);
    }

    Table table(tableName, columns);
    cout << "Table '" << tableName << "' created successfully.\n";
}

void handleQuery(const string &query)
{
    string trimmed = query;
    trimmed.erase(0, trimmed.find_first_not_of(" \t\n\r\f\v"));
    trimmed.erase(trimmed.find_last_not_of(" \t\n\r\f\v") + 1);
    string upper = trimmed;
    transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
    if (upper == "BEGIN" || upper == "BEGIN;")
    {
        Context::getTransaction().begin();
        cout << "Transaction started.\n";
        return;
    }
    if (upper == "COMMIT" || upper == "COMMIT;" && Context::getTransaction().inTransaction)
    {
        Context::getTransaction().commit();
        cout << "Transaction committed.\n";
        return;
    }
    if (upper == "ROLLBACK" || upper == "ROLLBACK;" && Context::getTransaction().inTransaction)
    {
        Context::getTransaction().rollback();
        cout << "Transaction rolled back.\n";
        return;
    }
    smatch match;
    regex insertPattern(R"(INSERT INTO (\w+)\s+VALUES\s*\((.+)\);?)", regex::icase);
    if (regex_match(query, match, insertPattern) && Context::getTransaction().inTransaction)
    {
        string tableName = match[1];
        string valuesRaw = match[2];
        vector<string> values;
        stringstream ss(valuesRaw);
        string val;
        while (getline(ss, val, ','))
        {
            val.erase(remove(val.begin(), val.end(), '\"'), val.end());
            val.erase(0, val.find_first_not_of(" \t\n\r\f\v"));
            val.erase(val.find_last_not_of(" \t\n\r\f\v") + 1);
            values.push_back(val);
        }
        Context::getTransaction().addInsertOperation(tableName, values);
        cout << "INSERT operation logged.\n";
        return;
    }
    regex updatePattern(R"(UPDATE\s+(\w+)\s+SET\s+(\w+)\s*=\s*\"?([^\"]+?)\"?\s+WHERE\s+(\w+)\s*(=|>=|<=|>|<)\s*\"?([^\"]+?)\"?;?)", regex::icase);
    if (regex_match(query, match, updatePattern) && Context::getTransaction().inTransaction)
    {
        string tableName = match[1];
        string column = match[2];
        string newValue = match[3];
        string conditionColumn = match[4];
        string compareOp = match[5];
        string conditionValue = match[6];

        vector<string> oldValues = {"<old_value>"}; 
        vector<string> newValues = {newValue};

        Context::getTransaction().addUpdateOperation(tableName, oldValues, newValues, conditionColumn, conditionValue, column, compareOp);
        cout << "UPDATE operation logged.\n";
        return;
    }
    regex deletePattern(R"(KILL FROM (\w+)\s+WHERE\s+(\w+)\s*=\s*\"?([^\"]+)\"?;?)", regex::icase);
    if (regex_match(query, match, deletePattern) && Context::getTransaction().inTransaction)
    {
        string tableName = match[1];
        string conditionColumn = match[2];
        string conditionValue = match[3];
        vector<string> oldValues = {"<old_deleted_row>"};
        Context::getTransaction().addDeleteOperation(tableName, oldValues, conditionColumn, conditionValue);
        cout << "DELETE operation logged.\n";
        return;
    }
    string upperQuery = query;
    transform(upperQuery.begin(), upperQuery.end(), upperQuery.begin(), ::toupper);

    if (upperQuery.find("CREATE TABLE") == 0)
        handleCreate(query);
    else if (upperQuery.find("INSERT INTO") == 0)
        handleInsert(query);
    else if (upperQuery.find("FIND * FROM") == 0)
        handleSelect(query);
    else if (upperQuery.find("UPDATE") == 0)
        handleUpdate(query);
    else if (upperQuery.find("KILL FROM") == 0)
        handleDelete(query);
    else
        cout << "Unsupported or invalid query.\n";
}

void handleInsert(const string &query)
{
    regex pattern(R"(INSERT INTO (\w+)\s+VALUES\s*\((.+)\);?)", regex::icase);
    smatch match;
    if (!regex_match(query, match, pattern))
    {
        cout << "Invalid INSERT syntax.\n";
        return;
    }
    string tableName = match[1];
    string valuesRaw = match[2];
    vector<string> values;
    stringstream ss(valuesRaw);
    string val;
    while (getline(ss, val, ','))
    {
        val.erase(remove(val.begin(), val.end(), '\"'), val.end());
        val.erase(0, val.find_first_not_of(" \t\n\r\f\v"));
        val.erase(val.find_last_not_of(" \t\n\r\f\v") + 1);
        values.push_back(val);
    }
    try
    {
        Table table = Table::loadFromSchema(tableName);
        table.insert(values, "data/" + tableName + ".db");
        cout << "Inserted into '" << tableName << "' successfully.\n";
    }
    catch (const exception &e)
    {
        cout << e.what() << "\n";
    }
}

void handleSelect(const string &query)
{
    // Pattern for complex WHERE conditions with logical operators
    regex patternComplexWhere(R"(FIND\s+\*\s+FROM\s+(\w+)\s+WHERE\s+((?:\w+\s*[=><]=?\s*\"?[^\"\s]+\"?\s*(?:AND|OR|NOT)\s*)+);?)", regex::icase);
    regex patternWithWhere(R"(FIND\s+\*\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s*([=><]=?)\s*\"?([^\"\s]+)\"?;?\s*)", regex::icase);
    regex patternNoWhere(R"(FIND\s+\*\s+FROM\s+(\w+)\s*;?\s*)", regex::icase);
    smatch match;

    if (regex_match(query, match, patternComplexWhere))
    {
        string tableName = match[1];
        string whereClause = match[2];
        
        if (tableName.empty())
        {
            cout << "Error: Table name is empty.\n";
            return;
        }

        // Parse the complex where clause
        vector<string> whereColumns;
        vector<string> compareOps;
        vector<string> whereValues;
        vector<string> logicalOps;

        regex conditionPattern(R"((\w+)\s*([=><]=?)\s*\"?([^\"\s]+)\"?\s*(AND|OR|NOT)?)");
        sregex_iterator it(whereClause.begin(), whereClause.end(), conditionPattern);
        sregex_iterator end;

        while (it != end)
        {
            smatch condition = *it;
            whereColumns.push_back(condition[1]);
            compareOps.push_back(condition[2]);
            whereValues.push_back(condition[3]);
            if (condition[4].matched)
            {
                logicalOps.push_back(condition[4]);
            }
            ++it;
        }

        try
        {
            Table table = Table::loadFromSchema(tableName);
            table.selectWhereComplex(tableName, whereColumns, compareOps, whereValues, logicalOps);
        }
        catch (const exception &e)
        {
            cout << "Error: " << e.what() << "\n";
        }
    }
    else if (regex_match(query, match, patternWithWhere))
    {
        string tableName = match[1];
        string whereCol = match[2];
        string compareOp = match[3];
        string whereVal = match[4];
        if (tableName.empty())
        {
            cout << "Error: Table name is empty.\n";
            return;
        }
        try
        {
            Table table = Table::loadFromSchema(tableName);
            table.selectWhere(tableName, whereCol, compareOp, whereVal);
        }
        catch (const exception &e)
        {
            cout << "Error: " << e.what() << "\n";
        }
    }
    else if (regex_match(query, match, patternNoWhere))
    {
        string tableName = match[1];

        if (tableName.empty())
        {
            cout << "Error: Table name is empty.\n";
            return;
        }

        try
        {
            Table table = Table::loadFromSchema(tableName);
            table.selectAll(tableName);
        }
        catch (const exception &e)
        {
            cout << "Error: " << e.what() << "\n";
        }
    }
    else
    {
        cout << "Invalid FIND syntax.\n";
    }
}

void handleUpdate(const string &query)
{
    regex pattern(R"(UPDATE\s+(\w+)\s+SET\s+(\w+)\s*=\s*\"?([^\"]+?)\"?\s+WHERE\s+(\w+)\s*(=|>=|<=|>|<)\s*\"?([^\"]+?)\"?;?)", regex::icase);
    smatch match;

    if (!regex_match(query, match, pattern))
    {
        cout << "Invalid UPDATE syntax.\n";
        return;
    }

    string tableName = match[1];
    string colToUpdate = match[2];
    string newVal = match[3];
    string whereCol = match[4];
    string compareOp = match[5];
    string whereVal = match[6];

    try
    {
        Table table = Table::loadFromSchema(tableName);
        string filePath = "data/" + tableName + ".db";
        table.update(colToUpdate, newVal, whereCol, compareOp, whereVal, filePath);
        cout << "Updated successfully.\n";
    }
    catch (const exception &e)
    {
        cout << "Update failed: " << e.what() << "\n";
    }
}

void handleDelete(const string &query)
{
    regex pattern(R"(KILL FROM (\w+) WHERE (\w+)\s*=\s*\"?([^\"\s]+)\"?;?)", regex::icase);
    smatch match;
    if (!regex_match(query, match, pattern))
    {
        cout << "Invalid DELETE syntax.\n";
        return;
    }
    string tableName = match[1], whereCol = match[2], whereVal = match[3];
    try
    {
        Table table = Table::loadFromSchema(tableName);
        table.deleteWhere(whereCol, whereVal, "data/" + Context::getTableName() + ".db");
        cout << "Deleted successfully.\n";
    }
    catch (const exception &e)
    {
        cout << e.what() << "\n";
    }
}