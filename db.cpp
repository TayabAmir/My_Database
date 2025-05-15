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
#include <filesystem>
#include <set>

using namespace std;

// BPlusTree implementation with fixed splitChild and insertNonFull
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
            {
                delete child;
            }
        }
    };
    Node *root;
    string colType;

    int compareKeys(const string &a, const string &b)
    {
        if (colType == "INT")
        {
            try
            {
                int ia = stoi(a), ib = stoi(b);
                return ia < ib ? -1 : (ia > ib ? 1 : 0);
            }
            catch (...)
            {
                return a.compare(b);
            }
        }
        return a.compare(b);
    }

    void splitChild(Node *parent, int index, Node *child)
    {
        cout << "[BPlusTree] Splitting child at index " << index << "\n";
        if (!parent || !child)
        {
            throw runtime_error("Null parent or child in splitChild");
        }
        if (child->keys.size() < ORDER)
        {
            throw runtime_error("Child node not full in splitChild");
        }

        Node *newNode = new Node(child->isLeaf);
        newNode->next = child->next;
        child->next = newNode;

        int mid = (ORDER - 1) / 2;        // Middle key index
        string midKey = child->keys[mid]; // Key to move to parent

        // Move keys and values after mid to newNode
        newNode->keys.assign(child->keys.begin() + mid + 1, child->keys.end());
        newNode->values.assign(child->values.begin() + mid + 1, child->values.end());

        // Truncate child, keeping keys up to mid
        child->keys.resize(mid);
        child->values.resize(mid);

        if (!child->isLeaf)
        {
            // Move children after mid to newNode
            newNode->children.assign(child->children.begin() + mid + 1, child->children.end());
            child->children.resize(mid + 1);
        }

        // Insert midKey into parent
        parent->keys.insert(parent->keys.begin() + index, midKey);
        parent->values.insert(parent->values.begin() + index, vector<uint64_t>());
        parent->children.insert(parent->children.begin() + index + 1, newNode);

        cout << "[BPlusTree] Split complete: child keys = " << child->keys.size() << ", newNode keys = " << newNode->keys.size() << "\n";
    }

    void insertNonFull(Node *node, const string &key, uint64_t offset)
    {
        cout << "[BPlusTree] InsertNonFull: key = " << key << ", node keys = " << node->keys.size() << "\n";
        if (!node)
        {
            throw runtime_error("Null node in insertNonFull");
        }
        int i = node->keys.size() - 1;
        if (node->isLeaf)
        {
            // Handle duplicates (e.g., dept_id)
            for (size_t j = 0; j < node->keys.size(); ++j)
            {
                if (node->keys[j] == key)
                {
                    cout << "[BPlusTree] Appending offset to existing key " << key << "\n";
                    node->values[j].push_back(offset);
                    return;
                }
            }
            // Insert new key
            node->keys.push_back("");
            node->values.push_back({});
            while (i >= 0 && compareKeys(key, node->keys[i]) < 0)
            {
                node->keys[i + 1] = node->keys[i];
                node->values[i + 1] = node->values[i];
                i--;
            }
            node->keys[i + 1] = key;
            node->values[i + 1].push_back(offset);
        }
        else
        {
            while (i >= 0 && compareKeys(key, node->keys[i]) < 0)
            {
                i--;
            }
            i++;
            if (i >= static_cast<int>(node->children.size()))
            {
                throw runtime_error("Invalid child index " + to_string(i) + " in insertNonFull");
            }
            if (!node->children[i])
            {
                throw runtime_error("Null child at index " + to_string(i));
            }
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

public:
    BPlusTree(const string &type) : root(new Node(true)), colType(type) {}
    ~BPlusTree() { delete root; }

    vector<uint64_t> search(const string &key)
    {
        cout << "[BPlusTree] Searching for key: " << key << "\n";
        Node *node = root;
        if (!node)
        {
            cout << "[BPlusTree] Root is null\n";
            return {};
        }
        while (node)
        {
            int i = 0;
            while (i < node->keys.size() && compareKeys(key, node->keys[i]) > 0)
            {
                i++;
            }
            if (i < node->keys.size() && node->keys[i] == key)
            {
                cout << "[BPlusTree] Found key " << key << " with " << node->values[i].size() << " offsets\n";
                return node->values[i];
            }
            if (node->isLeaf)
            {
                cout << "[BPlusTree] Key " << key << " not found\n";
                return {};
            }
            if (i >= static_cast<int>(node->children.size()))
            {
                cout << "[BPlusTree] Invalid child index " << i << "\n";
                return {};
            }
            if (!node->children[i])
            {
                cout << "[BPlusTree] Null child at index " << i << "\n";
                return {};
            }
            node = node->children[i];
        }
        cout << "[BPlusTree] Key " << key << " not found\n";
        return {};
    }

    void insert(const string &key, uint64_t offset)
    {
        try
        {
            if (!root)
            {
                root = new Node(true);
            }
            if (root->keys.size() == ORDER)
            {
                Node *newRoot = new Node(false);
                newRoot->children.push_back(root);
                root = newRoot;
                splitChild(newRoot, 0, newRoot->children[0]);
            }
            insertNonFull(root, key, offset);
        }
        catch (const exception &e)
        {
            throw runtime_error("BPlusTree insert failed for key '" + key + "': " + e.what());
        }
    }

    void save(ofstream &out)
    {
        if (!out.is_open())
        {
            throw runtime_error("Output stream is not open for saving BPlusTree");
        }
        try
        {
            saveNode(root, out);
        }
        catch (const exception &e)
        {
            throw runtime_error("Failed to save BPlusTree");
        }
    }

    void saveNode(Node *node, ofstream &out)
    {
        if (!node)
            return;
        out.write(reinterpret_cast<char *>(&node->isLeaf), sizeof(bool));
        size_t keyCount = node->keys.size();
        out.write(reinterpret_cast<char *>(&keyCount), sizeof(size_t));
        for (size_t i = 0; i < keyCount; ++i)
        {
            size_t keySize = node->keys[i].size();
            out.write(reinterpret_cast<char *>(&keySize), sizeof(size_t));
            out.write(node->keys[i].c_str(), keySize);
            size_t valueCount = node->values[i].size();
            out.write(reinterpret_cast<char *>(&valueCount), sizeof(size_t));
            for (uint64_t val : node->values[i])
            {
                out.write(reinterpret_cast<char *>(&val), sizeof(uint64_t));
            }
        }
        if (!node->isLeaf)
        {
            size_t childCount = node->children.size();
            out.write(reinterpret_cast<char *>(&childCount), sizeof(size_t));
            for (Node *child : node->children)
            {
                saveNode(child, out);
            }
        }
    }

    void load(ifstream &in)
    {
        if (!in.is_open())
        {
            clear();
            root = new Node(true);
            return;
        }
        try
        {
            delete root;
            root = loadNode(in);
            if (!root)
            {
                root = new Node(true);
            }
        }
        catch (const exception &e)
        {
            clear();
            root = new Node(true);
        }
    }

    Node *loadNode(ifstream &in)
    {
        if (!in.good())
            return nullptr;
        bool isLeaf;
        in.read(reinterpret_cast<char *>(&isLeaf), sizeof(bool));
        if (!in.good())
            return nullptr;
        Node *node = new Node(isLeaf);
        size_t keyCount;
        in.read(reinterpret_cast<char *>(&keyCount), sizeof(size_t));
        if (!in.good())
        {
            delete node;
            return nullptr;
        }
        node->keys.resize(keyCount);
        node->values.resize(keyCount);
        for (size_t i = 0; i < keyCount; ++i)
        {
            size_t keySize;
            in.read(reinterpret_cast<char *>(&keySize), sizeof(size_t));
            if (!in.good())
            {
                delete node;
                return nullptr;
            }
            char *keyBuffer = new char[keySize + 1];
            in.read(keyBuffer, keySize);
            keyBuffer[keySize] = '\0';
            node->keys[i] = keyBuffer;
            delete[] keyBuffer;
            size_t valueCount;
            in.read(reinterpret_cast<char *>(&valueCount), sizeof(size_t));
            node->values[i].resize(valueCount);
            for (size_t j = 0; j < valueCount; ++j)
            {
                uint64_t val;
                in.read(reinterpret_cast<char *>(&val), sizeof(uint64_t));
                if (!in.good())
                {
                    delete node;
                    return nullptr;
                }
                node->values[i][j] = val;
            }
        }
        if (!isLeaf)
        {
            size_t childCount;
            in.read(reinterpret_cast<char *>(&childCount), sizeof(size_t));
            if (!in.good())
            {
                delete node;
                return nullptr;
            }
            node->children.resize(childCount);
            for (size_t i = 0; i < childCount; ++i)
            {
                node->children[i] = loadNode(in);
                if (!node->children[i])
                {
                    delete node;
                    return nullptr;
                }
            }
        }
        return node;
    }

    void clear()
    {
        delete root;
        root = nullptr;
    }
};

Table::Table(const string &name, const vector<Column> &cols, const string &dbName)
{
    columns = cols;
    tableName = name;
    string dbPath = "databases/" + dbName + "/data/";
    filePath = dbPath + name + ".db";
    schemaPath = dbPath + name + ".schema";
    _mkdir("databases");
    _mkdir(("databases/" + dbName).c_str());
    _mkdir(dbPath.c_str());
    ofstream file(filePath, ios::out | ios::app);
    if (!file)
    {
        throw runtime_error("Failed to create or open file: " + filePath);
    }
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

Table Table::loadFromSchema(const string &tableName, const string &dbName)
{
    string schemaPath = "databases/" + dbName + "/data/" + tableName + ".schema";
    ifstream schema(schemaPath);
    if (!schema)
    {
        throw runtime_error("Schema for table '" + tableName + "' not found in database '" + dbName + "'.");
    }

    vector<Column> cols;
    string line;
    while (getline(schema, line))
    {
        if (line.empty())
        {
            continue;
        }

        istringstream iss(line);
        Column col;
        string token;

        iss >> col.name >> token;
        smatch strMatch;
        if (regex_match(token, strMatch, regex(R"(STRING\((\d+)\))", regex::icase)))
        {
            col.type = "STRING";
            col.size = stoi(strMatch[1]);
        }
        else if (token == "INT")
        {
            col.type = "INT";
            iss >> col.size;
        }
        else
        {
            schema.close();
            throw runtime_error("Invalid type in schema for column '" + col.name + "': " + token);
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
                    schema.close();
                    throw runtime_error("Invalid FOREIGN KEY reference format for column '" + col.name + "'");
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
            else
            {
                schema.close();
                throw runtime_error("Invalid constraint '" + token + "' for column '" + col.name + "'");
            }
        }

        cols.push_back(col);
    }
    schema.close();

    Table table(tableName, cols, dbName);
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
    if (!schema)
    {
        throw runtime_error("Failed to open schema file for writing: " + schemaPath);
    }
    for (const auto &col : columns)
    {
        schema << col.name << " " << col.type;
        if (col.type == "STRING")
        {
            schema << "(" << col.size << ")";
        }
        else
        {
            schema << " " << col.size;
        }
        if (col.isPrimaryKey)
        {
            schema << " PRIMARY_KEY";
        }
        if (col.isForeignKey)
        {
            if (col.refTable.empty() || col.refColumn.empty())
            {
                schema.close();
                throw runtime_error("Invalid FOREIGN_KEY for column '" + col.name + "': refTable or refColumn is empty.");
            }
            schema << " FOREIGN_KEY " << col.refTable << " " << col.refColumn;
        }
        if (col.isUnique)
        {
            schema << " UNIQUE_KEY";
        }
        if (col.isNotNull)
        {
            schema << " NOT_NULL";
        }
        if (col.isIndexed)
        {
            schema << " INDEXED";
        }
        schema << "\n";
    }
    schema.close();
}

// New helper function to check if a primary key value is referenced by foreign keys
bool Table::checkForeignKeyReferences(const string &pkColumn, const string &pkValue, string &errorTable, string &errorColumn)
{
    string dbName = Context::getInstance().getCurrentDatabase();
    string dataPath = "databases/" + dbName + "/data";

    // Find the primary key column
    string pkColName;
    for (const auto &col : columns)
    {
        if (col.isPrimaryKey)
        {
            pkColName = col.name;
            break;
        }
    }
    if (pkColName.empty())
    {
        return false; // No primary key, no references to check
    }

    // Scan all tables in the database
    for (const auto &entry : filesystem::directory_iterator(dataPath))
    {
        if (entry.path().extension() == ".schema")
        {
            string depTableName = entry.path().stem().string();
            if (depTableName == tableName)
            {
                continue; // Skip the current table
            }

            try
            {
                Table depTable = Table::loadFromSchema(depTableName, dbName);
                for (const auto &col : depTable.columns)
                {
                    if (col.isForeignKey && col.refTable == tableName && col.refColumn == pkColName)
                    {
                        // Check if the foreign key column contains pkValue
                        if (depTable.indexes.find(col.name) != depTable.indexes.end())
                        {
                            auto offsets = depTable.indexes[col.name]->search(pkValue);
                            if (!offsets.empty())
                            {
                                errorTable = depTableName;
                                errorColumn = col.name;
                                return true;
                            }
                        }
                        else
                        {
                            // Fallback: Scan the dependent table
                            vector<vector<string>> depRows = depTable.selectAll(depTableName);
                            int fkColIndex = -1;
                            for (size_t i = 0; i < depTable.columns.size(); ++i)
                            {
                                if (depTable.columns[i].name == col.name)
                                {
                                    fkColIndex = i;
                                    break;
                                }
                            }
                            if (fkColIndex == -1)
                            {
                                continue;
                            }
                            for (const auto &row : depRows)
                            {
                                if (row[fkColIndex] == pkValue)
                                {
                                    errorTable = depTableName;
                                    errorColumn = col.name;
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
            catch (const exception &e)
            {
                // Skip tables that fail to load
                continue;
            }
        }
    }
    return false;
}

bool Table::insert(const vector<string> &values, string filePath)
{
    if (values.size() != columns.size())
    {
        throw runtime_error("Incorrect number of values provided for table '" + tableName + "'. Expected " + to_string(columns.size()) + ", got " + to_string(values.size()) + ".");
    }

    for (size_t i = 0; i < values.size(); ++i)
    {
        const Column &col = columns[i];
        const string &value = values[i];

        // Validate NOT NULL
        if (col.isNotNull && value.empty())
        {
            throw runtime_error("Column '" + col.name + "' in table '" + tableName + "' cannot be null.");
        }

        // Validate type
        if (col.type == "INT")
        {
            try
            {
                stoi(value);
            }
            catch (const invalid_argument &)
            {
                throw runtime_error("Invalid INT value '" + value + "' for column '" + col.name + "' in table '" + tableName + "'.");
            }
        }
        else if (col.type == "STRING")
        {
            if (value.length() > col.size)
            {
                throw runtime_error("STRING value '" + value + "' exceeds size limit of " + to_string(col.size) + " for column '" + col.name + "' in table '" + tableName + "'.");
            }
        }

        // Validate UNIQUE or PRIMARY KEY
        if (col.isUnique || col.isPrimaryKey)
        {
            if (indexes.find(col.name) != indexes.end())
            {
                auto offsets = indexes[col.name]->search(value);
                if (!offsets.empty())
                {
                    throw runtime_error((col.isPrimaryKey ? "Primary key" : "Unique") + string(" value '") + value + "' already exists in column '" + col.name + "' of table '" + tableName + "'.");
                }
            }
            else
            {
                // Fallback: Scan table if no index
                vector<vector<string>> rows = selectAll(tableName);
                for (const auto &row : rows)
                {
                    if (row[i] == value)
                    {
                        throw runtime_error((col.isPrimaryKey ? "Primary key" : "Unique") + string(" value '") + value + "' already exists in column '" + col.name + "' of table '" + tableName + "'.");
                    }
                }
            }
        }

        // Validate foreign key
        if (col.isForeignKey)
        {
            try
            {
                Table refTable = Table::loadFromSchema(col.refTable, Context::getInstance().getCurrentDatabase());
                bool isRefPrimaryKey = false;
                for (const auto &refCol : refTable.columns)
                {
                    if (refCol.name == col.refColumn && refCol.isPrimaryKey)
                    {
                        isRefPrimaryKey = true;
                        break;
                    }
                }
                if (!isRefPrimaryKey)
                {
                    throw runtime_error("Referenced column '" + col.refColumn + "' in table '" + col.refTable + "' is not a primary key.");
                }
                if (refTable.indexes.find(col.refColumn) != refTable.indexes.end())
                {
                    auto offsets = refTable.indexes[col.refColumn]->search(value);
                    if (offsets.empty())
                    {
                        throw runtime_error("Foreign key value '" + value + "' in column '" + col.name + "' does not exist in referenced table '" + col.refTable + "' column '" + col.refColumn + "'.");
                    }
                }
                else
                {
                    vector<vector<string>> refRows = refTable.selectAll(col.refTable);
                    bool found = false;
                    int refColIndex = -1;
                    for (size_t j = 0; j < refTable.columns.size(); ++j)
                    {
                        if (refTable.columns[j].name == col.refColumn)
                        {
                            refColIndex = j;
                            break;
                        }
                    }
                    if (refColIndex == -1)
                    {
                        throw runtime_error("Referenced column '" + col.refColumn + "' not found in table '" + col.refTable + "'.");
                    }
                    for (const auto &row : refRows)
                    {
                        if (row[refColIndex] == value)
                        {
                            found = true;
                            break;
                        }
                    }
                    if (!found)
                    {
                        throw runtime_error("Foreign key value '" + value + "' in column '" + col.name + "' does not exist in referenced table '" + col.refTable + "' column '" + col.refColumn + "'.");
                    }
                }
            }
            catch (const runtime_error &e)
            {
                throw runtime_error("Foreign key validation failed for column '" + col.name + "': " + e.what());
            }
        }
    }

    ofstream file(filePath, ios::binary | ios::app);
    if (!file)
    {
        throw runtime_error("Failed to open file for writing: " + filePath);
    }
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

    return true;
}

bool Table::deleteWhere(const string &conditionExpr, const string &filePath)
{
    if (!conditionExpr.empty())
        if (!validateWhereClauseColumns(conditionExpr))
            return false;
    if (conditionExpr.empty())
    {
        cout << "Condition is required";
        return false;
    }
    ifstream in(filePath, ios::binary);
    if (!in)
    {
        throw runtime_error("Failed to open file for reading: " + filePath);
    }
    size_t rowSize = 0;
    for (auto &col : columns)
    {
        rowSize += col.size;
    }
    vector<vector<string>> remainingRows;
    vector<char> buffer(rowSize);
    bool deleted = false;

    // Find primary key column
    string pkColName;
    int pkColIndex = -1;
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (columns[i].isPrimaryKey)
        {
            pkColName = columns[i].name;
            pkColIndex = i;
            break;
        }
    }

    // Check for foreign key references before deletion
    if (!pkColName.empty())
    {
        vector<vector<string>> rowsToDelete;
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
            if (evaluateCondition(conditionExpr, row))
            {
                rowsToDelete.push_back(row);
            }
        }
        in.clear();
        in.seekg(0);

        for (const auto &row : rowsToDelete)
        {
            string pkValue = row[pkColIndex];
            string errorTable, errorColumn;
            if (checkForeignKeyReferences(pkColName, pkValue, errorTable, errorColumn))
            {
                in.close();
                throw runtime_error("Cannot delete primary key value '" + pkValue + "' from table '" + tableName + "' because it is referenced by foreign key in table '" + errorTable + "' column '" + errorColumn + "'.");
            }
        }
    }

    // Proceed with deletion
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
        else
        {
            deleted = true;
        }
    }
    in.close();

    ofstream out(filePath, ios::binary | ios::trunc);
    if (!out)
    {
        throw runtime_error("Failed to open file for writing: " + filePath);
    }
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

    if (deleted)
    {
        for (const auto &col : columns)
        {
            if (col.isIndexed)
            {
                rebuildIndex(col.name);
            }
        }
    }

    return true;
}

bool Table::createIndex(const string &colName)
{
    auto it = find_if(columns.begin(), columns.end(),
                      [&colName](const Column &col)
                      { return col.name == colName; });
    if (it == columns.end())
    {
        throw runtime_error("Column '" + colName + "' not found in table '" + tableName + "'.");
    }

    it->isIndexed = true;
    indexes[colName] = new BPlusTree(it->type);
    rebuildIndex(colName);
    saveSchema();
    return true;
}

void Table::loadIndex(const string &colName)
{
    cout << "[Table] Loading index for " << colName << "\n";
    auto it = find_if(columns.begin(), columns.end(),
                      [&colName](const Column &col)
                      { return col.name == colName; });
    if (it == columns.end())
    {
        cerr << "[Table] Column '" << colName << "' not found in table '" << tableName << "'\n";
        return;
    }

    string indexPath = "databases/" + Context::getInstance().getCurrentDatabase() + "/data/" + tableName + "." + colName + ".idx";
    indexes[colName] = new BPlusTree(it->type);

    // Fix: Create ifstream and pass to load
    ifstream in(indexPath, ios::binary);
    if (in.is_open())
    {
        try
        {
            indexes[colName]->load(in);
            cout << "[Table] Loaded index from " << indexPath << "\n";
        }
        catch (const exception &e)
        {
            cerr << "[Table] Failed to load index from " << indexPath << ": " << e.what() << "\n";
            in.close();
            delete indexes[colName];
            indexes[colName] = new BPlusTree(it->type); // Create new tree on load failure
        }
        in.close();
    }
    else
    {
        cout << "[Table] No existing index at " << indexPath << ", using new tree\n";
    }
}

void Table::saveIndex(const string &colName)
{
    cout << "[Table] Saving index for " << colName << "\n";
    string indexPath = "databases/" + Context::getInstance().getCurrentDatabase() + "/data/" + tableName + "." + colName + ".idx";
    if (indexes.find(colName) != indexes.end())
    {
        ofstream out(indexPath, ios::binary);
        if (!out.is_open())
        {
            throw runtime_error("Failed to open index file for writing: " + indexPath);
        }
        try
        {
            indexes[colName]->save(out);
            cout << "[Table] Index saved successfully to " << indexPath << "\n";
        }
        catch (const exception &e)
        {
            out.close();
            throw runtime_error("Failed to save index for column '" + colName + "' in table '" + tableName + "': " + e.what());
        }
        out.close();
    }
    else
    {
        cout << "[Table] No index found for column " << colName << "\n";
    }
}

void Table::rebuildIndex(const string &colName)
{
    if (indexes.find(colName) == indexes.end())
    {
        return;
    }
    indexes[colName]->clear();

    ifstream in(filePath, ios::binary);
    if (!in)
    {
        return;
    }

    size_t recordSize = 0;
    for (const auto &col : columns)
    {
        recordSize += col.size;
    }

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
    {
        in.close();
        return;
    }

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

vector<vector<string>> Table::selectAll(string tableName) const
{
    string dbName = Context::getInstance().getCurrentDatabase();
    ifstream file("databases/" + dbName + "/data/" + tableName + ".db", ios::binary);
    if (!file)
    {
        return {};
    }
    size_t recordSize = 0;
    for (auto &col : columns)
    {
        recordSize += col.size;
    }
    vector<char> buffer(recordSize);
    vector<vector<string>> result;
    while (file.read(buffer.data(), recordSize))
    {
        size_t offset = 0;
        vector<string> row;
        for (const auto &col : columns)
        {
            string val(buffer.data() + offset, col.size);
            size_t nullPos = val.find('\0');
            if (nullPos != string::npos)
                val.erase(nullPos);
            row.push_back(val);
            offset += col.size;
        }
        result.push_back(row);
    }
    file.close();
    return result;
}

bool Table::selectWhere(string tableName, const string &whereColumn, const string &compareOp, const string &whereValue)
{
    string dbName = Context::getInstance().getCurrentDatabase();
    string dataFilePath = "databases/" + dbName + "/data/" + tableName + ".db";

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
        return false;
    }

    if (indexes.find(whereColumn) != indexes.end() && compareOp == "=")
    {
        auto offsets = indexes[whereColumn]->search(whereValue);
        if (!offsets.empty())
        {
            ifstream file(dataFilePath, ios::binary);
            if (!file)
            {
                return false;
            }
            size_t recordSize = 0;
            for (auto &col : columns)
            {
                recordSize += col.size;
            }
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
            file.close();
            if (!foundMatches)
            {
                cout << "No records found matching the condition.\n";
            }
            return true;
        }
    }

    ifstream file(dataFilePath, ios::binary);
    if (!file)
    {
        return false;
    }
    size_t recordSize = 0;
    for (auto &col : columns)
    {
        recordSize += col.size;
    }
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
            file.close();
            return false;
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
    file.close();
    if (!foundMatches)
    {
        cout << "No records found matching the condition.\n";
    }
    return true;
}

bool Table::selectJoin(const string &table1Name, const Table &table2, const string &table2Name, const string &joinCondition)
{
    string dbName = Context::getInstance().getCurrentDatabase();
    vector<vector<string>> rows1 = selectAll(table1Name);
    vector<vector<string>> rows2 = table2.selectAll(table2Name);

    regex conditionPattern(R"(\s*(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)\s*)", regex::icase);
    smatch match;
    if (!regex_match(joinCondition, match, conditionPattern))
    {
        return false;
    }
    string t1Name = match[1];
    string col1 = match[2];
    string t2Name = match[3];
    string col2 = match[4];

    if (t1Name != table1Name || t2Name != table2Name)
    {
        return false;
    }
    int col1Index = -1, col2Index = -1;
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (columns[i].name == col1)
        {
            col1Index = i;
        }
    }
    for (size_t i = 0; i < table2.columns.size(); ++i)
    {
        if (table2.columns[i].name == col2)
        {
            col2Index = i;
        }
    }
    if (col1Index == -1 || col2Index == -1)
    {
        return false;
    }

    bool useIndex = (indexes.find(col1) != indexes.end() || table2.indexes.find(col2) != table2.indexes.end());
    vector<vector<string>> result;

    if (useIndex && indexes.find(col1) != indexes.end())
    {
        cout << "Using index for join\n";
        for (const auto &row2 : rows2)
        {
            string val2 = row2[col2Index];
            auto offsets = indexes[col1]->search(val2);
            if (!offsets.empty())
            {
                ifstream file("databases/" + dbName + "/data/" + table1Name + ".db", ios::binary);
                if (!file)
                {
                    return false;
                }
                size_t recordSize = 0;
                for (const auto &col : columns)
                {
                    recordSize += col.size;
                }
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
                        {
                            combinedRow.push_back(val);
                        }
                        for (const auto &val : row2)
                        {
                            combinedRow.push_back(val);
                        }
                        result.push_back(combinedRow);
                    }
                }
                file.close();
            }
        }
    }
    else
    {
        cout << "Using nested loop join\n";
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
                    {
                        combinedRow.push_back(val);
                    }
                    for (const auto &val : row2)
                    {
                        combinedRow.push_back(val);
                    }
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

    return true;
}

bool Table::update(const string &colToUpdate, const string &newVal, const string &whereClause, const string &filePath)
{
    // Validate that colToUpdate exists
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
        throw runtime_error("Column '" + colToUpdate + "' does not exist in table '" + tableName + "'.");
    }
    if (!whereClause.empty())
        if (!validateWhereClauseColumns(whereClause))
            return false;
    ifstream in(filePath, ios::binary);
    if (!in)
    {
        throw runtime_error("Failed to open file for reading: " + filePath);
    }
    size_t rowSize = 0;
    for (auto &col : columns)
    {
        rowSize += col.size;
    }
    vector<vector<string>> allRows;
    vector<char> buffer(rowSize);
    bool updated = false;
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

        if (whereClause.empty() || evaluateCondition(whereClause, row))
        {
            row[updateIndex] = newVal;
            updated = true;
        }

        allRows.push_back(row);
    }
    in.close();

    ofstream out(filePath, ios::binary | ios::trunc);
    if (!out)
    {
        throw runtime_error("Failed to open file for writing: " + filePath);
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

    if (updated)
    {
        for (const auto &col : columns)
        {
            if (col.isIndexed)
            {
                rebuildIndex(col.name);
            }
        }
    }
    return true;
}

bool Table::validateWhereClauseColumns(const string &whereClause)
{
    string normalizedClause = regex_replace(whereClause, regex(R"(\bAND\b)", regex::icase), "&&");
    normalizedClause = regex_replace(normalizedClause, regex(R"(\bOR\b)", regex::icase), "||");
    normalizedClause = regex_replace(normalizedClause, regex(R"(\bNOT\b)", regex::icase), "!");
    vector<string> tokens = tokenize(normalizedClause);

    set<string> operators = {"=", "!=", ">", "<", ">=", "<=", "&&", "||", "!", "(", ")"};

    for (const auto &token : tokens)
    {
        if (operators.count(token) > 0)
        {
            continue;
        }
        if ((token.front() == '"' && token.back() == '"') ||
            (token.front() == '\'' && token.back() == '\'') ||
            regex_match(token, regex(R"(\d+)")))
        {
            continue;
        }

        bool found = false;
        for (const auto &col : columns)
            if (col.name == token)
                return true;
        if (!found)
            return false;
    }
    return true;
}

bool Table::selectWhereWithExpression(const string &tableName, const string &whereClause)
{
    string dbName = Context::getInstance().getCurrentDatabase();
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

        bool foundMatches = false;
        for (const auto &row : rows)
        {
            if (evaluateCondition(whereClause, row))
            {
                foundMatches = true;
                for (size_t i = 0; i < row.size(); ++i)
                {
                    cout << setw(table.columns[i].size) << row[i] << " | ";
                }
                cout << endl;
            }
        }
        if (!foundMatches)
        {
            cout << "No records found matching the condition.\n";
        }
        return true;
    }
    catch (const exception &e)
    {
        return false;
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

string Table::getTableName()
{
    return tableName;
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