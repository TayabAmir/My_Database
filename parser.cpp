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

bool validateForeignKey(const string &refTable, const string &refColumn, const string &dbName)
{
    try
    {
        Table table = Table::loadFromSchema(refTable, dbName);
        for (const auto &col : table.columns)
        {
            if (col.name == refColumn)
            {
                return true;
            }
        }
        cout << "Error: Referenced column '" << refColumn << "' does not exist in table '" << refTable << "'.\n";
        return false;
    }
    catch (const exception &e)
    {
        cout << "Error: Referenced table '" << refTable << "' does not exist in database '" << dbName << "'.\n";
        return false;
    }
}

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
        Context::getInstance().getTransaction().isDatabaseGiven = true;
    }
    else
    {
        cout << "Error: Database '" << dbName << "' does not exist.\n";
    }
}

void handleShowTables(const string &query)
{
    string dbName = Context::getInstance().getCurrentDatabase();
    if (dbName.empty())
    {
        cout << "Error: No database selected. Use USE <database_name> to select a database.\n";
        return;
    }

    string dataPath = "databases/" + dbName + "/data";
    if (!filesystem::exists(dataPath))
    {
        cout << "Error: Database data directory not found.\n";
        return;
    }

    vector<string> tables;
    for (const auto &entry : filesystem::directory_iterator(dataPath))
    {
        if (entry.path().extension() == ".schema")
        {
            string tableName = entry.path().stem().string();
            tables.push_back(tableName);
        }
    }

    if (tables.empty())
    {
        cout << "No tables found in database '" << dbName << "'.\n";
        return;
    }

    cout << left << setw(30) << "Table Name" << "|\n";
    cout << string(30, '-') << "-+\n";
    for (const auto &table : tables)
    {
        cout << left << setw(30) << table << "|\n";
    }
}

void handleDescribeTable(const string &query)
{
    regex pattern(R"(DESCRIBE\s+(\w+);?|DESC\s+(\w+);?)", regex::icase);
    smatch match;
    if (!regex_match(query, match, pattern))
    {
        cout << "Invalid DESCRIBE syntax.\n";
        return;
    }
    string tableName = match[1].str().empty() ? match[2].str() : match[1].str();
    string dbName = Context::getInstance().getCurrentDatabase();
    if (dbName.empty())
    {
        cout << "Error: No database selected. Use USE <database_name> to select a database.\n";
        return;
    }

    try
    {
        Table table = Table::loadFromSchema(tableName, dbName);
        cout << "Table: " << tableName << "\n";
        cout << left
             << setw(20) << "Column Name" << "| "
             << setw(15) << "Type" << "| "
             << setw(10) << "Size" << "| "
             << setw(15) << "Constraints" << "|\n";
        cout << string(20, '-') << "-+-"
             << string(15, '-') << "-+-"
             << string(10, '-') << "-+-"
             << string(15, '-') << "-+\n";

        for (const auto &col : table.columns)
        {
            string type = col.type;
            if (col.type == "STRING")
            {
                type += "(" + to_string(col.size) + ")";
            }
            string constraints;
            if (col.isPrimaryKey)
                constraints += "PRIMARY_KEY ";
            if (col.isForeignKey)
                constraints += "FOREIGN_KEY ";
            if (col.isUnique)
                constraints += "UNIQUE ";
            if (col.isNotNull)
                constraints += "NOT_NULL ";
            if (col.isIndexed)
                constraints += "INDEXED ";
            constraints = constraints.empty() ? "NONE" : constraints.substr(0, constraints.size() - 1);

            cout << left
                 << setw(20) << col.name << "| "
                 << setw(15) << type << "| "
                 << setw(10) << col.size << "| "
                 << setw(15) << constraints << "|\n";
        }
    }
    catch (const exception &e)
    {
        cout << "Error: " << e.what() << "\n";
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
    if (dbName.empty())
    {
        cout << "Error: No database selected. Use USE <database_name> to select a database.\n";
        return;
    }
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
                string name, typeFull, token;
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

                // Parse constraints
                vector<string> tokens;
                while (colStream >> token)
                {
                    tokens.push_back(token);
                }

                for (size_t i = 0; i < tokens.size(); ++i)
                {
                    token = tokens[i];
                    if (token == "PRIMARY" || token == "PRIMARY_KEY")
                    {
                        if (token == "PRIMARY" && i + 1 < tokens.size() && tokens[i + 1] == "KEY")
                        {
                            ++i; // Skip "KEY"
                        }
                        column.isPrimaryKey = true;
                    }
                    else if (token == "FOREIGN" || token == "FOREIGN_KEY")
                    {
                        column.isForeignKey = true;
                        if (token == "FOREIGN" && i + 1 < tokens.size() && tokens[i + 1] == "KEY")
                        {
                            ++i; // Skip "KEY"
                        }
                        if (i + 1 < tokens.size() && tokens[i + 1] == "REFERENCES")
                        {
                            ++i; // Skip "REFERENCES"
                            if (i + 1 < tokens.size())
                            {
                                string refs = tokens[++i];
                                smatch fkMatch;
                                if (regex_match(refs, fkMatch, regex(R"((\w+)\((\w+)\))")))
                                {
                                    column.refTable = fkMatch[1];
                                    column.refColumn = fkMatch[2];
                                    // Validate foreign key reference
                                    if (!validateForeignKey(column.refTable, column.refColumn, dbName))
                                    {
                                        return;
                                    }
                                }
                                else
                                {
                                    cout << "Invalid FOREIGN KEY reference format. Expected table(column).\n";
                                    return;
                                }
                            }
                            else
                            {
                                cout << "Expected table(column) after REFERENCES.\n";
                                return;
                            }
                        }
                        else
                        {
                            cout << "Expected REFERENCES after FOREIGN KEY.\n";
                            return;
                        }
                    }
                    else if (token == "UNIQUE" || token == "UNIQUE_KEY")
                    {
                        column.isUnique = true;
                    }
                    else if (token == "NOT" || token == "NOT_NULL")
                    {
                        if (token == "NOT" && i + 1 < tokens.size() && tokens[i + 1] == "NULL")
                        {
                            ++i; // Skip "NULL"
                        }
                        column.isNotNull = true;
                    }
                    else if (token == "INDEXED")
                    {
                        column.isIndexed = true;
                    }
                }

                columns.push_back(column);
            }
            start = pos + 1;
        }
        pos++;
    }

    // Handle the last column definition
    if (start < columnsRaw.length())
    {
        string colDef = columnsRaw.substr(start);
        colDef.erase(0, colDef.find_first_not_of(" \t\n\r\f\v"));
        colDef.erase(colDef.find_last_not_of(" \t\n\r\f\v") + 1);

        if (!colDef.empty())
        {
            stringstream colStream(colDef);
            string name, typeFull, token;
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

            // Parse constraints
            vector<string> tokens;
            while (colStream >> token)
            {
                tokens.push_back(token);
            }

            for (size_t i = 0; i < tokens.size(); ++i)
            {
                token = tokens[i];
                if (token == "PRIMARY" || token == "PRIMARY_KEY")
                {
                    if (token == "PRIMARY" && i + 1 < tokens.size() && tokens[i + 1] == "KEY")
                    {
                        ++i; // Skip "KEY"
                    }
                    column.isPrimaryKey = true;
                }
                else if (token == "FOREIGN" || token == "FOREIGN_KEY")
                {
                    column.isForeignKey = true;
                    if (token == "FOREIGN" && i + 1 < tokens.size() && tokens[i + 1] == "KEY")
                    {
                        ++i; // Skip "KEY"
                    }
                    if (i + 1 < tokens.size() && tokens[i + 1] == "REFERENCES")
                    {
                        ++i; // Skip "REFERENCES"
                        if (i + 1 < tokens.size())
                        {
                            string refs = tokens[++i];
                            smatch fkMatch;
                            if (regex_match(refs, fkMatch, regex(R"((\w+)\((\w+)\))")))
                            {
                                column.refTable = fkMatch[1];
                                column.refColumn = fkMatch[2];
                                // Validate foreign key reference
                                if (!validateForeignKey(column.refTable, column.refColumn, dbName))
                                {
                                    return;
                                }
                            }
                            else
                            {
                                cout << "Invalid FOREIGN KEY reference format. Expected table(column).\n";
                                return;
                            }
                        }
                        else
                        {
                            cout << "Expected table(column) after REFERENCES.\n";
                            return;
                        }
                    }
                    else
                    {
                        cout << "Expected REFERENCES after FOREIGN KEY.\n";
                        return;
                    }
                }
                else if (token == "UNIQUE" || token == "UNIQUE_KEY")
                {
                    column.isUnique = true;
                }
                else if (token == "NOT" || token == "NOT_NULL")
                {
                    if (token == "NOT" && i + 1 < tokens.size() && tokens[i + 1] == "NULL")
                    {
                        ++i; // Skip "NULL"
                    }
                    column.isNotNull = true;
                }
                else if (token == "INDEXED")
                {
                    column.isIndexed = true;
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
        if (table.createIndex(colName))
        {
            cout << "Index created on column '" << colName << "' for table '" << tableName << "' in database '" << dbName << "'.\n";
        }
        else
        {
            cout << "Error: Failed to create index on column '" << colName << "' (column not found or schema error).\n";
        }
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
        string filePath = "databases/" + dbName + "/data/" + tableName + ".db";
        if (table.insert(values, filePath))
            cout << "Inserted into '" << tableName << "' in database '" << dbName << "' successfully.\n";
        else
            cout << "Error: Failed to insert into '" << tableName << "' (invalid values, constraints violated, or file error).\n";
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
            if (!table1.selectJoin(table1Name, table2, table2Name, joinCondition))
                cout << "Error: Failed to perform join (invalid condition or table/column not found).\n";
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
            if (!table.selectWhereWithExpression(tableName, whereClause))
                cout << "Error: Failed to select with WHERE clause (invalid table or condition).\n";
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
            if (rows.empty())
            {
                cout << "No records found.\n";
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
        if (table.update(colToUpdate, newVal, whereClause, filePath))
            cout << "Updated successfully in database '" << dbName << "'.\n";
        else
            cout << "Error: Failed to update '" << tableName << "' (invalid column, file error, or no matching rows).\n";
    }
    catch (const exception &e)
    {
        cout << "Error: " << e.what() << "\n";
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
        string filePath = "databases/" + dbName + "/data/" + tableName + ".db";
        if (table.deleteWhere(conditionExpr, filePath))
            cout << "Deleted successfully from database '" << dbName << "'.\n";
        else
            cout << "Error: Failed to delete from '" << tableName << "' (file error or invalid condition).\n";
    }
    catch (const exception &e)
    {
        cout << "Error: " << e.what() << "\n";
    }
}

void handleQuery(const string &query)
{
    string trimmed = query;
    trimmed.erase(0, trimmed.find_first_not_of(" \t\n\r\f\v"));
    trimmed.erase(trimmed.find_last_not_of(" \t\n\r\f\v") + 1);
    string upper = trimmed;
    transform(upper.begin(), upper.end(), upper.begin(), ::toupper);

    if (upper == "CLS" || upper == "CLS;")
    {
        system("cls");
        return;
    }

    // Transaction basic commands
    if (upper == "BEGIN" || upper == "BEGIN;")
    {
        Context::getInstance().getTransaction().begin();
        return;
    }
    if ((upper == "COMMIT" || upper == "COMMIT;") && Context::getInstance().getTransaction().inTransaction)
    {
        Context::getInstance().getTransaction().commit();
        return;
    }
    if ((upper == "ROLLBACK" || upper == "ROLLBACK;") && Context::getInstance().getTransaction().inTransaction)
    {
        Context::getInstance().getTransaction().rollback();
        return;
    }

    // Transaction checkpoint commands
    smatch cpMatch;
    regex createCheckpointPattern(R"(CHECKPOINT\s+CREATE\s+(\w+);?)", regex::icase);
    if (regex_match(query, cpMatch, createCheckpointPattern) && Context::getInstance().getTransaction().inTransaction)
    {
        string checkpointId = cpMatch[1];
        Context::getInstance().getTransaction().createCheckpoint(checkpointId);
        return;
    }

    regex rollbackCheckpointPattern(R"(CHECKPOINT\s+ROLLBACK\s+TO\s+(\w+);?)", regex::icase);
    if (regex_match(query, cpMatch, rollbackCheckpointPattern) && Context::getInstance().getTransaction().inTransaction)
    {
        string checkpointId = cpMatch[1];
        Context::getInstance().getTransaction().rollbackToCheckpoint(checkpointId);
        return;
    }

    regex commitCheckpointPattern(R"(CHECKPOINT\s+COMMIT\s+TO\s+(\w+);?)", regex::icase);
    if (regex_match(query, cpMatch, commitCheckpointPattern) && Context::getInstance().getTransaction().inTransaction)
    {
        string checkpointId = cpMatch[1];
        Context::getInstance().getTransaction().commitToCheckpoint(checkpointId);
        return;
    }

    regex listCheckpointsPattern(R"(CHECKPOINT\s+LIST;?)", regex::icase);
    if (regex_match(query, listCheckpointsPattern) && Context::getInstance().getTransaction().inTransaction)
    {
        auto checkpoints = Context::getInstance().getTransaction().listCheckpoints();
        if (checkpoints.empty())
        {
            cout << "No checkpoints exist in the current transaction.\n";
        }
        else
        {
            cout << "Checkpoints in current transaction:\n";
            for (const auto &cp : checkpoints)
            {
                cout << "- " << cp;
                if (cp == Context::getInstance().getTransaction().getCurrentCheckpoint())
                {
                    cout << " (current)";
                }
                cout << "\n";
            }
        }
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
    if (upper.find("USE") == 0 && !Context::getInstance().getTransaction().isDatabaseGiven && Context::getInstance().getTransaction().inTransaction)
    {
        handleUseDatabase(query);
        Context::getInstance().getTransaction().isDatabaseGiven = true;
        return;
    }
    if (upper.find("USE") == 0 && Context::getInstance().getTransaction().isDatabaseGiven && Context::getInstance().getTransaction().inTransaction)
    {
        cout << "Transaction on only one database is allowed\n";
        return;
    }
    if (upper.find("USE") == 0)
    {
        handleUseDatabase(query);
        return;
    }
    if (upper.find("SHOW TABLES") == 0)
    {
        handleShowTables(query);
        return;
    }
    if (upper.find("DESCRIBE ") == 0 || upper.find("DESC ") == 0)
    {
        handleDescribeTable(query);
        return;
    }

    // Regular DML operations
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