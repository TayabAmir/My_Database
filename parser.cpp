#include "parser.hpp"
#include "db.hpp"
#include <sstream>
#include <iostream>
#include <regex>
#include "Transaction.hpp"
#include "global.hpp"
#include <iomanip>
#include <fstream>
#include <direct.h>
#include <filesystem>

using namespace std;

void handleCreateDatabase(const string &query)
{
    regex pattern(R"(CREATE DATABASE (\w+);?)", regex::icase);
    smatch match;
    if (!regex_match(query, match, pattern))
    {
        cout << "Invalid CREATE DATABASE syntax.\n";
        return;
    }
    string dbName = match[1];
    string dbPath = "databases/" + dbName;
    string dataPath = dbPath + "/data";
    _mkdir("databases");
    if (_mkdir(dbPath.c_str()) == 0 && _mkdir(dataPath.c_str()) == 0)
    {
        cout << "Database '" << dbName << "' created successfully.\n";
    }
    else
    {
        cout << "Error: Database '" << dbName << "' already exists or cannot be created.\n";
    }
}

void handleUseDatabase(const string &query)
{
    regex pattern(R"(USE (\w+);?)", regex::icase);
    smatch match;
    if (!regex_match(query, match, pattern))
    {
        cout << "Invalid USE syntax.\n";
        return;
    }
    string dbName = match[1];
    string dbPath = "databases/" + dbName;
    if (filesystem::exists(dbPath))
    {
        Context::getInstance().setCurrentDatabase(dbName);
        cout << "Switched to database '" << dbName << "'.\n";
    }
    else
    {
        cout << "Error: Database '" << dbName << "' does not exist.\n";
    }
}

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
    string dbName = Context::getInstance().getCurrentDatabase();
    if (dbName == "default" && !filesystem::exists("databases/default"))
    {
        handleCreateDatabase("CREATE DATABASE default;");
    }

    columnsRaw.erase(remove(columnsRaw.begin(), columnsRaw.end(), '\n'), columnsRaw.end());
    columnsRaw.erase(remove(columnsRaw.begin(), columnsRaw.end(), '\r'), columnsRaw.end());
    columnsRaw.erase(remove(columnsRaw.begin(), columnsRaw.end(), '\t'), columnsRaw.end());

    vector<Column> columns;
    size_t pos = 0;
    size_t start = 0;
    bool inParentheses = false;

    while (pos < columnsRaw.length())
    {
        if (columnsRaw[pos] == '(')
        {
            inParentheses = true;
        }
        else if (columnsRaw[pos] == ')')
        {
            inParentheses = false;
        }
        else if (columnsRaw[pos] == ',' && !inParentheses)
        {
            string colDef = columnsRaw.substr(start, pos - start);
            colDef.erase(0, colDef.find_first_not_of(" \t\n\r\f\v"));
            colDef.erase(colDef.find_last_not_of(" \t\n\r\f\v") + 1);

            if (!colDef.empty())
            {
                stringstream colStream(colDef);
                string name, typeFull, constraint;
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
                    column.size = 10;
                }
                else
                {
                    cout << "Unknown type: " << typeFull << "\n";
                    return;
                }

                string token;
                while (colStream >> token)
                {
                    if (token == "PRIMARY" || token == "PRIMARY_KEY")
                    {
                        string next;
                        colStream >> next;
                        if (next == "KEY" || token == "PRIMARY_KEY")
                            column.isPrimaryKey = true;
                    }
                    else if (token == "FOREIGN" || token == "FOREIGN_KEY")
                    {
                        column.isForeignKey = true;
                        string next;
                        colStream >> next;
                        if (next == "KEY")
                        {
                            string refs;
                            colStream >> refs >> refs;

                            smatch fkMatch;
                            if (regex_match(refs, fkMatch, regex(R"((\w+)\((\w+)\))")))
                            {
                                column.isForeignKey = true;
                                column.refTable = fkMatch[1];
                                column.refColumn = fkMatch[2];
                            }
                            else
                            {
                                cout << "Invalid FOREIGN KEY reference format.\n";
                                return;
                            }
                        }
                    }
                    else if (token == "UNIQUE" || token == "UNIQUE_KEY")
                    {
                        column.isUnique = true;
                    }
                    else if (token == "NOT" || token == "NOT_NULL")
                    {
                        string next;
                        colStream >> next;
                        if (next == "NULL" || token == "NOT_NULL")
                            column.isNotNull = true;
                    }
                }

                columns.push_back(column);
            }
            start = pos + 1;
        }
        pos++;
    }

    if (start < columnsRaw.length())
    {
        string colDef = columnsRaw.substr(start);
        colDef.erase(0, colDef.find_first_not_of(" \t\n\r\f\v"));
        colDef.erase(colDef.find_last_not_of(" \t\n\r\f\v") + 1);

        if (!colDef.empty())
        {
            stringstream colStream(colDef);
            string name, typeFull, constraint;
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
                column.size = 10;
            }
            else
            {
                cout << "Unknown type: " << typeFull << "\n";
                return;
            }

            string token;
            while (colStream >> token)
            {
                if (token == "PRIMARY" || token == "PRIMARY_KEY")
                {
                    string next;
                    colStream >> next;
                    if (next == "KEY" || token == "PRIMARY_KEY")
                        column.isPrimaryKey = true;
                }
                else if (token == "FOREIGN" || token == "FOREIGN_KEY")
                {
                    string next;
                    colStream >> next;
                    if (next == "KEY")
                    {
                        string refs;
                        colStream >> refs >> refs;

                        smatch fkMatch;
                        if (regex_match(refs, fkMatch, regex(R"((\w+)\((\w+)\))")))
                        {
                            column.isForeignKey = true;
                            column.refTable = fkMatch[1];
                            column.refColumn = fkMatch[2];
                        }
                        else
                        {
                            cout << "Invalid FOREIGN KEY reference format.\n";
                            return;
                        }
                    }
                }
                else if (token == "UNIQUE" || token == "UNIQUE_KEY")
                {
                    column.isUnique = true;
                }
                else if (token == "NOT" || token == "NOT_NULL")
                {
                    string next;
                    colStream >> next;
                    if (next == "NULL" || token == "NOT_NULL")
                        column.isNotNull = true;
                }
            }

            columns.push_back(column);
        }
    }

    try
    {
        Table table(tableName, columns, dbName);
        cout << "Table '" << tableName << "' created successfully in database '" << dbName << "' with " << table.columns.size() << " columns.\n";
    }
    catch (const exception &e)
    {
        cout << "Error: " << e.what() << "\n";
    }
}

void handleCreateIndex(const string &query)
{
    regex pattern(R"(CREATE INDEX ON (\w+)\s*\((\w+)\);?)", regex::icase);
    smatch match;
    if (!regex_match(query, match, pattern))
    {
        cout << "Invalid CREATE INDEX syntax.\n";
        return;
    }
    string tableName = match[1];
    string colName = match[2];
    string dbName = Context::getInstance().getCurrentDatabase();

    try
    {
        Table table = Table::loadFromSchema(tableName, dbName);
        table.createIndex(colName);
        cout << "Index created on column '" << colName << "' for table '" << tableName << "' in database '" << dbName << "'.\n";
    }
    catch (const exception &e)
    {
        cout << "Error: " << e.what() << "\n";
    }
}

void handleInsert(const string &query)
{
    regex pattern(R"(INSERT\s+INTO\s+(\w+)\s+VALUES\s*\(\s*([^)]+)\s*\);?)", regex::icase);
    smatch match;
    if (!regex_match(query, match, pattern))
    {
        cout << "Invalid INSERT syntax.\n";
        return;
    }
    string tableName = match[1];
    string valuesRaw = match[2];
    string dbName = Context::getInstance().getCurrentDatabase();
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
        Table table = Table::loadFromSchema(tableName, dbName);
        table.insert(values, "databases/" + dbName + "/data/" + tableName + ".db");
        cout << "Inserted into '" << tableName << "' in database '" << dbName << "' successfully.\n";
    }
    catch (const exception &e)
    {
        cout << "Error: " << e.what() << "\n";
    }
}

void handleSelect(const string &query)
{
    regex patternNoWhere(R"(FIND\s+\*\s+FROM\s+(\w+)\s*;?\s*)", regex::icase);
    regex patternWithWhere(R"(FIND\s+\*\s+FROM\s+(\w+)\s+WHERE\s+(.+);?\s*)", regex::icase);
    regex patternJoin(R"(FIND\s+\*\s+FROM\s+(\w+)\s+JOIN\s+(\w+)\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)\s*;?\s*)", regex::icase);
    smatch match;
    string dbName = Context::getInstance().getCurrentDatabase();

    if (regex_match(query, match, patternJoin))
    {
        string table1Name = match[1];
        string table2Name = match[2];
        string table1Col = match[4];
        string table2Col = match[6];
        string joinCondition = match[3].str() + "." + match[4].str() + " = " + match[5].str() + "." + match[6].str();
        if (table1Name.empty() || table2Name.empty())
        {
            cout << "Error: Table name is empty.\n";
            return;
        }

        try
        {
            Table table1 = Table::loadFromSchema(table1Name, dbName);
            Table table2 = Table::loadFromSchema(table2Name, dbName);
            table1.selectJoin(table1Name, table2, table2Name, joinCondition);
        }
        catch (const exception &e)
        {
            cout << "Error: " << e.what() << "\n";
        }
    }
    else if (regex_match(query, match, patternWithWhere))
    {
        string tableName = match[1];
        string whereClause = match[2];
        if (tableName.empty())
        {
            cout << "Error: Table name is empty.\n";
            return;
        }

        try
        {
            Table table = Table::loadFromSchema(tableName, dbName);
            table.selectWhereWithExpression(tableName, whereClause);
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
            Table table = Table::loadFromSchema(tableName, dbName);
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
                for (size_t i = 0; i < row.size(); ++i)
                {
                    cout << setw(table.columns[i].size) << row[i] << " | ";
                }
                cout << endl;
            }
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
    regex pattern(R"(UPDATE\s+(\w+)\s+SET\s+(\w+)\s*=\s*\"?([^\"]+?)\"?\s+WHERE\s+(.+);?)", regex::icase);
    smatch match;
    string dbName = Context::getInstance().getCurrentDatabase();

    if (!regex_match(query, match, pattern))
    {
        cout << "Invalid UPDATE syntax.\n";
        return;
    }

    string tableName = match[1];
    string colToUpdate = match[2];
    string newVal = match[3];
    string whereClause = match[4];

    try
    {
        Table table = Table::loadFromSchema(tableName, dbName);
        string filePath = "databases/" + dbName + "/data/" + tableName + ".db";
        table.update(colToUpdate, newVal, whereClause, filePath);
        cout << "Updated successfully in database '" << dbName << "'.\n";
    }
    catch (const exception &e)
    {
        cout << "Update failed: " << e.what() << "\n";
    }
}

void handleDelete(const string &query)
{
    regex pattern(R"(KILL\s+FROM\s+(\w+)\s+WHERE\s+(.+);?)", regex::icase);
    smatch match;
    string dbName = Context::getInstance().getCurrentDatabase();

    if (!regex_match(query, match, pattern))
    {
        cout << "Invalid DELETE syntax.\n";
        return;
    }

    string tableName = match[1];
    string conditionExpr = match[2];

    try
    {
        Table table = Table::loadFromSchema(tableName, dbName);
        table.deleteWhere(conditionExpr, "databases/" + dbName + "/data/" + tableName + ".db");
        cout << "Deleted successfully from database '" << dbName << "'.\n";
    }
    catch (const exception &e)
    {
        cout << e.what() << "\n";
    }
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
        Context::getInstance().getTransaction().begin();
        cout << "Transaction started.\n";
        return;
    }
    if ((upper == "COMMIT" || upper == "COMMIT;") && Context::getInstance().getTransaction().inTransaction)
    {
        Context::getInstance().getTransaction().commit();
        cout << "Transaction committed.\n";
        return;
    }
    if ((upper == "ROLLBACK" || upper == "ROLLBACK;") && Context::getInstance().getTransaction().inTransaction)
    {
        Context::getInstance().getTransaction().rollback();
        cout << "Transaction rolled back.\n";
        return;
    }

    if (upper.find("CREATE DATABASE") == 0 && !Context::getInstance().getTransaction().inTransaction)
    {
        handleCreateDatabase(query);
        return;
    }
    if (upper.find("CREATE DATABASE") == 0 && Context::getInstance().getTransaction().inTransaction)
    {
        cout << "CREATE DATABASE command is not allowed inside a transaction.\n";
        return;
    }
    if (upper.find("USE") == 0 && !Context::getInstance().getTransaction().isDatabaseGiven)
    {
        handleUseDatabase(query);
        Context::getInstance().getTransaction().isDatabaseGiven = true;
        return;
    }
    if (upper.find("USE") == 0 && Context::getInstance().getTransaction().isDatabaseGiven)
    {
        cout << "Transaction on only one database is allowed\n";
        return;
    }
    if (upper.find("USE") == 0)
    {
        handleUseDatabase(query);
        return;
    }

    smatch match;
    regex insertPattern(R"(INSERT INTO (\w+)\s+VALUES\s*\((.+)\);?)", regex::icase);
    if (regex_match(query, match, insertPattern))
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

        if (Context::getInstance().getTransaction().inTransaction && Context::getInstance().getTransaction().isDatabaseGiven)
        {
            Context::getInstance().getTransaction().addInsertOperation(tableName, values);
            cout << "INSERT operation logged.\n";
        }
        else if (Context::getInstance().getTransaction().inTransaction && !Context::getInstance().getTransaction().isDatabaseGiven)
        {
            cout << "No database selected. Use USE <database_name> to select a database.\n";
        }
        else
        {
            handleInsert(query);
        }
        return;
    }

    regex updatePattern(R"(UPDATE\s+(\w+)\s+SET\s+(\w+)\s*=\s*\"?([^\"]+?)\"?\s+WHERE\s+(\w+)\s*(=|>=|<=|>|<)\s*\"?([^\"]+?)\"?;?)", regex::icase);
    if (regex_match(query, match, updatePattern))
    {
        string tableName = match[1];
        string columnToUpdate = match[2];
        string newValue = match[3];
        string conditionColumn = match[4];
        string compareOp = match[5];
        string conditionValue = match[6];

        vector<string> newValues = {newValue};
        string whereClause = conditionColumn + " " + compareOp + " " + conditionValue;

        if (Context::getInstance().getTransaction().inTransaction && Context::getInstance().getTransaction().isDatabaseGiven)
        {
            Context::getInstance().getTransaction().addUpdateOperation(tableName, newValues, columnToUpdate, whereClause);
            cout << "UPDATE operation logged.\n";
        }
        else if (Context::getInstance().getTransaction().inTransaction && !Context::getInstance().getTransaction().isDatabaseGiven)
        {
            cout << "No database selected. Use USE <database_name> to select a database.\n";
        }
        else
        {
            handleUpdate(query);
        }
        return;
    }

    regex deletePattern(R"(KILL FROM (\w+)\s+WHERE\s+(\w+)\s*(=|>=|<=|>|<)\s*\"?([^\"]+)\"?;?)", regex::icase);
    if (regex_match(query, match, deletePattern))
    {
        string tableName = match[1];
        string conditionColumn = match[2];
        string compareOp = match[3];
        string conditionValue = match[4];

        string whereClause = conditionColumn + " " + compareOp + " " + conditionValue;

        if (Context::getInstance().getTransaction().inTransaction && Context::getInstance().getTransaction().isDatabaseGiven)
        {
            Context::getInstance().getTransaction().addDeleteOperation(tableName, whereClause);
            cout << "DELETE operation logged.\n";
        }
        else if (!Context::getInstance().getTransaction().isDatabaseGiven && !Context::getInstance().getTransaction().inTransaction)
        {
            cout << "No database selected. Use USE <database_name> to select a database.\n";
        }
        else
        {
            handleDelete(query);
        }
        return;
    }

    if (upper.find("CREATE TABLE") == 0)
        handleCreate(query);
    else if (upper.find("CREATE INDEX") == 0)
        handleCreateIndex(query);
    else if (upper.find("INSERT INTO") == 0)
        handleInsert(query);
    else if (upper.find("FIND * FROM") == 0)
        handleSelect(query);
    else if (upper.find("UPDATE") == 0)
        handleUpdate(query);
    else if (upper.find("KILL FROM") == 0)
        handleDelete(query);
    else
        cout << "Unsupported or invalid query.\n";
}