#include "db.hpp"
#include <fstream>
#include <iostream>
#include <direct.h>
#include <cstring>
#include "global.hpp"
#include <string>
#include <cstdint>
#include <vector>
#include <map>
#include <regex>
#include <stack>
#include <iomanip>
#include <sstream>
#include <functional>
#include <algorithm>

using namespace std;

class BPlusTree
{
private:
    static const int ORDER = 4;
    struct Node
    {
        vector<string> keys;
        vector<vector<uint64_t>> values;
        vector<Node *> children;
        bool isLeaf;
        Node *next;
        Node(bool leaf = true) : isLeaf(leaf), next(nullptr) {}
        ~Node()
        {
            for (Node *child : children)
                delete child;
        }
    };
    Node *root;
    string colType;

    void splitChild(Node *parent, int index, Node *child)
    {
        Node *newNode = new Node(child->isLeaf);
        newNode->next = child->next;
        child->next = newNode;

        int mid = ORDER / 2;
        newNode->keys.assign(child->keys.begin() + mid, child->keys.end());
        newNode->values.assign(child->values.begin() + mid, child->values.end());
        child->keys.resize(mid);
        child->values.resize(mid);

        if (!child->isLeaf)
        {
            newNode->children.assign(child->children.begin() + mid + 1, child->children.end());
            child->children.resize(mid + 1);
        }

        parent->keys.insert(parent->keys.begin() + index, child->keys[mid]);
        parent->values.insert(parent->values.begin() + index, vector<uint64_t>());
        parent->children.insert(parent->children.begin() + index + 1, newNode);
    }

    void insertNonFull(Node *node, const string &key, uint64_t offset)
    {
        int i = node->keys.size() - 1;
        if (node->isLeaf)
        {
            while (i >= 0 && compareKeys(key, node->keys[i]) < 0)
            {
                i--;
            }
            i++;
            auto it = find_if(node->keys.begin(), node->keys.end(),
                              [&](const string &k)
                              { return k == key; });
            if (it != node->keys.end())
            {
                int idx = distance(node->keys.begin(), it);
                node->values[idx].push_back(offset);
            }
            else
            {
                node->keys.insert(node->keys.begin() + i, key);
                node->values.insert(node->values.begin() + i, vector<uint64_t>{offset});
            }
        }
        else
        {
            while (i >= 0 && compareKeys(key, node->keys[i]) < 0)
            {
                i--;
            }
            i++;
            if (node->children[i]->keys.size() == ORDER)
            {
                splitChild(node, i, node->children[i]);
                if (compareKeys(key, node->keys[i]) > 0)
                {
                    i++;
                }
            }
            insertNonFull(node->children[i], key, offset);
        }
    }

    int compareKeys(const string &a, const string &b)
    {
        if (colType == "INT")
        {
            try
            {
                int ia = stoi(a);
                int ib = stoi(b);
                return (ia < ib) ? -1 : (ia > ib) ? 1
                                                  : 0;
            }
            catch (...)
            {
                return a.compare(b);
            }
        }
        return a.compare(b);
    }

    void saveNode(ofstream &out, Node *node)
    {
        uint8_t isLeaf = node->isLeaf ? 1 : 0;
        out.write(reinterpret_cast<char *>(&isLeaf), sizeof(isLeaf));
        uint32_t keyCount = node->keys.size();
        out.write(reinterpret_cast<char *>(&keyCount), sizeof(keyCount));

        for (size_t i = 0; i < node->keys.size(); ++i)
        {
            uint32_t keyLen = node->keys[i].size();
            out.write(reinterpret_cast<char *>(&keyLen), sizeof(keyLen));
            out.write(node->keys[i].c_str(), keyLen);
            uint32_t offsetCount = node->values[i].size();
            out.write(reinterpret_cast<char *>(&offsetCount), sizeof(offsetCount));
            out.write(reinterpret_cast<char *>(node->values[i].data()), offsetCount * sizeof(uint64_t));
        }

        if (!node->isLeaf)
        {
            uint32_t childCount = node->children.size();
            out.write(reinterpret_cast<char *>(&childCount), sizeof(childCount));
            for (Node *child : node->children)
            {
                saveNode(out, child);
            }
        }
    }

    Node *loadNode(ifstream &in)
    {
        uint8_t isLeaf;
        if (!in.read(reinterpret_cast<char *>(&isLeaf), sizeof(isLeaf)))
        {
            return nullptr;
        }
        Node *node = new Node(isLeaf != 0);
        uint32_t keyCount;
        in.read(reinterpret_cast<char *>(&keyCount), sizeof(keyCount));

        node->keys.resize(keyCount);
        node->values.resize(keyCount);
        for (uint32_t i = 0; i < keyCount; ++i)
        {
            uint32_t keyLen;
            in.read(reinterpret_cast<char *>(&keyLen), sizeof(keyLen));
            node->keys[i].resize(keyLen);
            in.read(&node->keys[i][0], keyLen);
            uint32_t offsetCount;
            in.read(reinterpret_cast<char *>(&offsetCount), sizeof(offsetCount));
            node->values[i].resize(offsetCount);
            in.read(reinterpret_cast<char *>(node->values[i].data()), offsetCount * sizeof(uint64_t));
        }

        if (!node->isLeaf)
        {
            uint32_t childCount;
            in.read(reinterpret_cast<char *>(&childCount), sizeof(childCount));
            node->children.resize(childCount);
            for (uint32_t i = 0; i < childCount; ++i)
            {
                node->children[i] = loadNode(in);
            }
        }
        return node;
    }

public:
    BPlusTree(const string &type) : root(new Node(true)), colType(type) {}
    ~BPlusTree() { delete root; }

    void insert(const string &key, uint64_t offset)
    {
        if (root->keys.size() == ORDER)
        {
            Node *newRoot = new Node(false);
            newRoot->children.push_back(root);
            root = newRoot;
            splitChild(newRoot, 0, root->children[0]);
        }
        insertNonFull(root, key, offset);
    }

    vector<uint64_t> search(const string &key)
    {
        Node *node = root;
        while (node)
        {
            int i = 0;
            while (i < node->keys.size() && compareKeys(key, node->keys[i]) > 0)
            {
                i++;
            }
            if (i < node->keys.size() && node->keys[i] == key)
            {
                return node->values[i];
            }
            if (node->isLeaf)
            {
                return {};
            }
            node = node->children[i];
        }
        return {};
    }

    void clear()
    {
        delete root;
        root = new Node(true);
    }

    void save(const string &filePath)
    {
        ofstream out(filePath, ios::binary | ios::trunc);
        if (!out)
            return;
        saveNode(out, root);
        out.close();
    }

    void load(const string &filePath)
    {
        ifstream in(filePath, ios::binary);
        if (!in)
            return;
        delete root;
        root = loadNode(in);
        in.close();
    }
};

// Table Implementation
Table::Table(const string &name, const vector<Column> &cols)
{
    columns = cols;
    filePath = "data/" + name + ".db";
    schemaPath = "data/" + name + ".schema";
    tableName = name;
    _mkdir("data");
    std::ofstream file(filePath, std::ios::out | std::ios::app);
    if (!file)
        throw std::runtime_error("Failed to create or open file: " + filePath);
    file.close();
    saveSchema();
}

Table::~Table()
{
    for (auto &[colName, tree] : indexes)
    {
        delete tree;
    }
}

Table Table::loadFromSchema(const string &tableName)
{
    ifstream schema("data/" + tableName + ".schema");
    if (!schema)
    {
        throw runtime_error("Schema for table '" + tableName + "' not found.");
    }

    vector<Column> cols;
    string line;
    while (getline(schema, line))
    {
        if (line.empty())
            continue;

        istringstream iss(line);
        Column col;
        string token;

        if (!(iss >> col.name >> col.type >> col.size))
        {
            throw runtime_error("Invalid schema format for column definition");
        }

        while (iss >> token)
        {
            if (token == "PRIMARY_KEY")
            {
                col.isPrimaryKey = true;
            }
            else if (token == "FOREIGN_KEY")
            {
                col.isForeignKey = true;
                if (!(iss >> col.refTable >> col.refColumn))
                {
                    throw runtime_error("Invalid FOREIGN KEY reference format");
                }
            }
            else if (token == "UNIQUE_KEY")
            {
                col.isUnique = true;
            }
            else if (token == "NOT_NULL")
            {
                col.isNotNull = true;
            }
            else if (token == "INDEXED")
            {
                col.isIndexed = true;
            }
        }

        cols.push_back(col);
    }

    Table table(tableName, cols);
    for (const auto &col : cols)
    {
        if (col.isIndexed)
        {
            table.loadIndex(col.name);
        }
    }

    return table;
}

void Table::saveSchema()
{
    ofstream schema(schemaPath);
    for (const auto &col : columns)
    {
        schema << col.name << " " << col.type << " " << col.size;
        if (col.isPrimaryKey)
            schema << " PRIMARY_KEY";
        if (col.isForeignKey)
            schema << " FOREIGN_KEY " << col.refTable << " " << col.refColumn;
        if (col.isUnique)
            schema << " UNIQUE_KEY";
        if (col.isNotNull)
            schema << " NOT_NULL";
        if (col.isIndexed)
            schema << " INDEXED";
        schema << "\n";
    }
    schema.close();
}

void Table::createIndex(const string &colName)
{
    auto it = find_if(columns.begin(), columns.end(),
                      [&colName](const Column &col)
                      { return col.name == colName; });
    if (it == columns.end())
    {
        cerr << "Column '" << colName << "' not found.\n";
        return;
    }

    it->isIndexed = true;
    indexes[colName] = new BPlusTree(it->type);
    rebuildIndex(colName);
    saveSchema();
}

void Table::selectJoin(const string &table1Name, Table &table2, const string &table2Name, const string &joinCondition)
{
    vector<vector<string>> rows1 = selectAll(table1Name);
    vector<vector<string>> rows2 = table2.selectAll(table2Name);

    regex conditionPattern(R"(\s*(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)\s*)", regex::icase);
    smatch match;
    if (!regex_match(joinCondition, match, conditionPattern))
    {
        cout << "Invalid join condition syntax.\n";
        return;
    }
    string t1Name = match[1];
    string col1 = match[2];
    string t2Name = match[3];
    string col2 = match[4];

    if (t1Name != table1Name || t2Name != table2Name)
    {
        cout << "Table names in join condition do not match.\n";
        return;
    }
    int col1Index = -1, col2Index = -1;
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (columns[i].name == col1)
            col1Index = i;
    }
    for (size_t i = 0; i < table2.columns.size(); ++i)
    {
        if (table2.columns[i].name == col2)
            col2Index = i;
    }
    if (col1Index == -1 || col2Index == -1)
    {
        cout << "Column not found in join condition.\n";
        return;
    }

    bool useIndex = (indexes.find(col1) != indexes.end() || table2.indexes.find(col2) != table2.indexes.end());
    vector<vector<string>> result;

    if (useIndex && indexes.find(col1) != indexes.end())
    {
        cout << "hello" << endl;
        for (const auto &row2 : rows2)
        {
            string val2 = row2[col2Index];
            auto offsets = indexes[col1]->search(val2);
            if (!offsets.empty())
            {
                ifstream file("data/" + table1Name + ".db", ios::binary);
                if (!file)
                {
                    cout << "Error reading data file for " << table1Name << ".\n";
                    return;
                }
                size_t recordSize = 0;
                for (const auto &col : columns)
                    recordSize += col.size;
                vector<char> buffer(recordSize);
                for (uint64_t offset : offsets)
                {
                    file.seekg(offset);
                    if (file.read(buffer.data(), recordSize))
                    {
                        vector<string> row1;
                        size_t colOffset = 0;
                        for (const auto &col : columns)
                        {
                            string val(buffer.data() + colOffset, col.size);
                            val.erase(val.find('\0'));
                            row1.push_back(val);
                            colOffset += col.size;
                        }
                        vector<string> combinedRow;
                        for (const auto &val : row1)
                            combinedRow.push_back(val);
                        for (const auto &val : row2)
                            combinedRow.push_back(val);
                        result.push_back(combinedRow);
                    }
                }
                file.close();
            }
        }
    }
    else
    {
        cout << "hello\n";
        for (const auto &row1 : rows1)
        {
            for (const auto &row2 : rows2)
            {
                string expr = table1Name + "." + col1 + " = " + table2Name + "." + col2;
                string processedExpr = expr;
                processedExpr = regex_replace(processedExpr, regex("\\b" + table1Name + "\\." + col1 + "\\b"), "\"" + row1[col1Index] + "\"");
                processedExpr = regex_replace(processedExpr, regex("\\b" + table2Name + "\\." + col2 + "\\b"), "\"" + row2[col2Index] + "\"");
                if (evalLogic(processedExpr))
                {
                    vector<string> combinedRow;
                    for (const auto &val : row1)
                        combinedRow.push_back(val);
                    for (const auto &val : row2)
                        combinedRow.push_back(val);
                    result.push_back(combinedRow);
                }
            }
        }
    }
    cout << left;
    for (const auto &col : columns)
    {
        cout << setw(col.size) << (table1Name + "." + col.name) << " | ";
    }
    for (const auto &col : table2.columns)
    {
        cout << setw(col.size) << (table2Name + "." + col.name) << " | ";
    }
    cout << endl;

    for (const auto &col : columns)
    {
        cout << string(col.size, '-') << "-+-";
    }
    for (const auto &col : table2.columns)
    {
        cout << string(col.size, '-') << "-+-";
    }
    cout << endl;

    for (const auto &row : result)
    {
        for (size_t i = 0; i < row.size(); ++i)
        {
            size_t size = (i < columns.size()) ? columns[i].size : table2.columns[i - columns.size()].size;
            cout << setw(size) << row[i] << " | ";
        }
        cout << endl;
    }
}

void Table::loadIndex(const string &colName)
{
    auto it = find_if(columns.begin(), columns.end(),
                      [&colName](const Column &col)
                      { return col.name == colName; });
    if (it == columns.end())
        return;

    string indexPath = "data/" + tableName + "." + colName + ".idx";
    indexes[colName] = new BPlusTree(it->type);
    indexes[colName]->load(indexPath);
}

void Table::saveIndex(const string &colName)
{
    string indexPath = "data/" + tableName + "." + colName + ".idx";
    if (indexes.find(colName) != indexes.end())
    {
        indexes[colName]->save(indexPath);
    }
}

void Table::rebuildIndex(const string &colName)
{
    if (indexes.find(colName) == indexes.end())
        return;
    indexes[colName]->clear();

    ifstream in(filePath, ios::binary);
    if (!in)
        return;

    size_t recordSize = 0;
    for (const auto &col : columns)
        recordSize += col.size;

    int colIndex = -1;
    size_t offset = 0;
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (columns[i].name == colName)
        {
            colIndex = i;
            break;
        }
        offset += columns[i].size;
    }
    if (colIndex == -1)
        return;

    vector<char> buffer(recordSize);
    uint64_t fileOffset = 0;
    while (in.read(buffer.data(), recordSize))
    {
        string val(buffer.data() + offset, columns[colIndex].size);
        val.erase(val.find('\0'));
        indexes[colName]->insert(val, fileOffset);
        fileOffset += recordSize;
    }
    in.close();
    saveIndex(colName);
}

void Table::insert(const vector<string> &values, string filePath)
{
    if (values.size() != columns.size())
    {
        cout << ("Number of values (" + to_string(values.size()) +
                 ") does not match number of columns (" +
                 to_string(columns.size()) + ")");
        return;
    }
    for (size_t i = 0; i < values.size(); ++i)
    {
        const Column &col = columns[i];
        const string &value = values[i];
        if (col.isNotNull && value.empty())
        {
            cout << "Column '" + col.name + "' cannot be NULL";
            return;
        }
        if (col.type == "INT")
        {
            try
            {
                stoi(value);
            }
            catch (const invalid_argument &)
            {
                cout << "Invalid INT value for column '" + col.name + "'";
                return;
            }
        }
        else if (col.type == "STRING")
        {
            if (value.length() > col.size)
            {
                cout << "String value too long for column '" + col.name +
                            "'. Maximum length is " + to_string(col.size);
                return;
            }
        }
        if (col.isUnique || col.isPrimaryKey)
        {
            if (indexes.find(col.name) != indexes.end())
            {
                auto offsets = indexes[col.name]->search(value);
                if (!offsets.empty())
                {
                    cout << "Duplicate value for " << (col.isPrimaryKey ? "PRIMARY KEY" : "UNIQUE") << " column '" + col.name + "'";
                    return;
                }
            }
        }
    }
    ofstream file(filePath, ios::binary | ios::app);
    uint64_t fileOffset = file.tellp();
    for (size_t i = 0; i < columns.size(); ++i)
    {
        char buffer[256] = {0};
        strncpy(buffer, values[i].c_str(), columns[i].size);
        file.write(buffer, columns[i].size);
    }
    file.close();

    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (columns[i].isIndexed)
        {
            indexes[columns[i].name]->insert(values[i], fileOffset);
            saveIndex(columns[i].name);
        }
    }
}

vector<vector<string>> Table::selectAll(string tableName)
{
    ifstream file("data/" + tableName + ".db", ios::binary);
    if (!file)
    {
        cerr << "Error reading data file.\n";
        return {};
    }
    size_t recordSize = 0;
    for (auto &col : columns)
        recordSize += col.size;
    vector<char> buffer(recordSize);
    vector<vector<string>> result;
    while (file.read(buffer.data(), recordSize))
    {
        size_t offset = 0;
        vector<string> row;
        for (const auto &col : columns)
        {
            string val(buffer.data() + offset, col.size);
            val.erase(val.find('\0'));
            row.push_back(val);
            offset += col.size;
        }
        result.push_back(row);
    }
    file.close();
    return result;
}

void Table::selectWhere(string tableName, const string &whereColumn, const string &compareOp, const string &whereValue)
{
    if (indexes.find(whereColumn) != indexes.end() && compareOp == "=")
    {
        auto offsets = indexes[whereColumn]->search(whereValue);
        if (!offsets.empty())
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
            bool foundMatches = false;
            cout << "Records where " << whereColumn << " = " << whereValue << ":\n";
            for (uint64_t offset : offsets)
            {
                file.seekg(offset);
                if (file.read(buffer.data(), recordSize))
                {
                    foundMatches = true;
                    size_t colOffset = 0;
                    for (const auto &col : columns)
                    {
                        string val(buffer.data() + colOffset, col.size);
                        val.erase(val.find('\0'));
                        cout << col.name << ": " << val << " ";
                        colOffset += col.size;
                    }
                    cout << "\n";
                }
            }
            if (!foundMatches)
                cout << "No records found matching the condition.\n";
            file.close();
            return;
        }
    }

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
    return tableName;
}

void Table::update(const string &colToUpdate, const string &newVal, const string &whereClause, const string &filePath)
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
    int updateIndex = -1;
    size_t updateOffset = 0;
    for (size_t i = 0; i < columns.size(); i++)
    {
        if (columns[i].name == colToUpdate)
        {
            updateIndex = i;
            break;
        }
        updateOffset += columns[i].size;
    }
    if (updateIndex == -1)
    {
        cerr << "Error: Column to update not found.\n";
        return;
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

        if (evaluateCondition(whereClause, row))
        {
            row[updateIndex] = newVal;
        }

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
    out.close();

    for (const auto &col : columns)
    {
        if (col.isIndexed)
        {
            rebuildIndex(col.name);
        }
    }
}

void Table::deleteWhere(const string &conditionExpr, const string &filePath)
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
        if (!evaluateCondition(conditionExpr, row))
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
    std::cout << "Deleted matching rows from: " << filePath << "\n";

    for (const auto &col : columns)
    {
        if (col.isIndexed)
        {
            rebuildIndex(col.name);
        }
    }
}

string Table::replaceValues(const string &expr, const vector<string> &row, const vector<Column> &columns)
{
    string result = expr;
    for (size_t i = 0; i < columns.size(); ++i)
    {
        string colPattern = "\\b" + columns[i].name + "\\b";
        result = regex_replace(result, regex(colPattern), "\"" + row[i] + "\"");
    }
    return result;
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
    string processedExpr = replaceValues(expr, row, columns);
    return evalLogic(processedExpr);
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
    auto isNumber = [](const string &s)
    {
        return !s.empty() && all_of(s.begin(), s.end(), ::isdigit);
    };

    if (isNumber(lhs) && isNumber(rhs))
    {
        int lhsNum = stoi(lhs);
        int rhsNum = stoi(rhs);

        if (compareOp == "=")
            return lhsNum == rhsNum;
        if (compareOp == "!=")
            return lhsNum != rhsNum;
        if (compareOp == ">")
            return lhsNum > rhsNum;
        if (compareOp == "<")
            return lhsNum < rhsNum;
        if (compareOp == ">=")
            return lhsNum >= rhsNum;
        if (compareOp == "<=")
            return lhsNum <= rhsNum;
    }
    else
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
    }

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