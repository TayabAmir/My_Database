#include "BPlusTree.hpp"
#include <stdexcept>
#include <iostream>
#include <algorithm>
#include <cctype>
#include <vector>

using namespace std;

BPlusTree::Node::Node(bool leaf) : isLeaf(leaf), next(nullptr) {}

BPlusTree::Node::~Node()
{
    for (Node *child : children)
    {
        delete child;
    }
}

BPlusTree::BPlusTree(const string &type) : root(new Node(true)), colType(type) {}

BPlusTree::~BPlusTree()
{
    delete root;
}

string trim(const std::string &str)
{
    string result = str;
    result.erase(result.begin(), find_if(result.begin(), result.end(), [](unsigned char c)
                                         { return !isspace(c); }));
    result.erase(find_if(result.rbegin(), result.rend(), [](unsigned char c)
                         { return !isspace(c); })
                     .base(),
                 result.end());
    return result;
}

int BPlusTree::compareKeys(const string &a, const string &b)
{
    string aTrimmed = trim(a), bTrimmed = trim(b);
    cout << "[BPlusTree] Comparing keys: '" << aTrimmed << "' vs '" << bTrimmed << "' (colType: " << colType << ")\n";
    if (colType == "INT")
    {
        try
        {
            int ia = stoi(aTrimmed), ib = stoi(bTrimmed);
            return ia < ib ? -1 : (ia > ib ? 1 : 0);
        }
        catch (const exception &e)
        {
            cout << "[BPlusTree] Failed to convert keys to int: a='" << aTrimmed << "', b='" << bTrimmed << "', error: " << e.what() << ", falling back to string compare\n";
            return aTrimmed.compare(bTrimmed);
        }
    }
    return aTrimmed.compare(bTrimmed);
}

void BPlusTree::splitChild(Node *parent, int index, Node *child)
{
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

    int mid = (ORDER - 1) / 2;
    string midKey = child->keys[mid];

    newNode->keys.assign(child->keys.begin() + mid + 1, child->keys.end());
    newNode->values.assign(child->values.begin() + mid + 1, child->values.end());

    child->keys.resize(mid);
    child->values.resize(mid);

    if (!child->isLeaf)
    {
        newNode->children.assign(child->children.begin() + mid + 1, child->children.end());
        child->children.resize(mid + 1);
    }

    parent->keys.insert(parent->keys.begin() + index, midKey);
    parent->values.insert(parent->values.begin() + index, vector<uint64_t>());
    parent->children.insert(parent->children.begin() + index + 1, newNode);
}

void BPlusTree::insertNonFull(Node *node, const string &key, uint64_t offset)
{
    if (!node)
    {
        throw runtime_error("Null node in insertNonFull");
    }
    string trimmedKey = trim(key);
    cout << "[BPlusTree] Inserting key: '" << trimmedKey << "', offset: " << offset << "\n";
    int i = node->keys.size() - 1;
    if (node->isLeaf)
    {
        for (size_t j = 0; j < node->keys.size(); ++j)
        {
            if (node->keys[j] == trimmedKey)
            {
                cout << "[BPlusTree] Appending offset " << offset << " to existing key " << trimmedKey << "\n";
                node->values[j].push_back(offset);
                return;
            }
        }
        node->keys.push_back("");
        node->values.push_back({});
        while (i >= 0 && compareKeys(trimmedKey, node->keys[i]) < 0)
        {
            node->keys[i + 1] = node->keys[i];
            node->values[i + 1] = node->values[i];
            i--;
        }
        node->keys[i + 1] = trimmedKey;
        node->values[i + 1].push_back(offset);
        cout << "[BPlusTree] Inserted new key " << trimmedKey << " at index " << i + 1 << "\n";
    }
    else
    {
        while (i >= 0 && compareKeys(trimmedKey, node->keys[i]) < 0)
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
            if (compareKeys(trimmedKey, node->keys[i]) > 0)
            {
                i++;
            }
        }
        insertNonFull(node->children[i], trimmedKey, offset);
    }
}

vector<uint64_t> BPlusTree::search(const string &key)
{
    string trimmedKey = trim(key);
    cout << "[BPlusTree] Searching for key: '" << trimmedKey << "' (colType: " << colType << ")\n";
    Node *node = root;
    if (!node)
    {
        cout << "[BPlusTree] Root is null\n";
        return {};
    }
    while (node)
    {
        int i = 0;
        while (i < node->keys.size() && compareKeys(trimmedKey, node->keys[i]) > 0)
        {
            i++;
        }
        if (i < node->keys.size() && node->keys[i] == trimmedKey)
        {
            cout << "[BPlusTree] Found key " << trimmedKey << " with " << node->values[i].size() << " offsets\n";
            return node->values[i];
        }
        if (node->isLeaf)
        {
            cout << "[BPlusTree] Key " << trimmedKey << " not found in leaf\n";
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
    cout << "[BPlusTree] Key " << trimmedKey << " not found\n";
    return {};
}

void BPlusTree::insert(const string &key, uint64_t offset)
{
    string trimmedKey = trim(key);
    cout << "[BPlusTree] Inserting key: '" << trimmedKey << "', offset: " << offset << "\n";
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
        insertNonFull(root, trimmedKey, offset);
        cout << "[BPlusTree] Insert completed for key: " << trimmedKey << "\n";
    }
    catch (const exception &e)
    {
        throw runtime_error("BPlusTree insert failed for key '" + trimmedKey + "': " + e.what());
    }
}

void BPlusTree::save(ofstream &out)
{
    cout << "[BPlusTree] Saving tree\n";
    if (!out.is_open())
    {
        throw runtime_error("Output stream is not open for saving BPlusTree");
    }
    try
    {
        saveNode(root, out);
        cout << "[BPlusTree] Tree saved successfully\n";
    }
    catch (const exception &e)
    {
        throw runtime_error("Failed to save BPlusTree");
    }
}

void BPlusTree::saveNode(Node *node, ofstream &out)
{
    if (!node)
        return;
    cout << "[BPlusTree] Saving node with " << node->keys.size() << " keys\n";
    out.write(reinterpret_cast<char *>(&node->isLeaf), sizeof(bool));
    size_t keyCount = node->keys.size();
    out.write(reinterpret_cast<char *>(&keyCount), sizeof(size_t));
    for (size_t i = 0; i < keyCount; ++i)
    {
        string trimmedKey = trim(node->keys[i]);
        size_t keySize = trimmedKey.size();
        out.write(reinterpret_cast<char *>(&keySize), sizeof(size_t));
        out.write(trimmedKey.c_str(), keySize);
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

void BPlusTree::collectKeys(Node *node, vector<string> &keys)
{
    if (!node)
        return;
    for (const auto &key : node->keys)
    {
        keys.push_back(key);
    }
    if (!node->isLeaf)
    {
        for (Node *child : node->children)
        {
            collectKeys(child, keys);
        }
    }
}

void BPlusTree::getAllKeys(vector<string> &keys)
{
    collectKeys(root, keys);
}

void BPlusTree::load(ifstream &in)
{
    cout << "[BPlusTree] Loading tree\n";
    if (!in.is_open())
    {
        cout << "[BPlusTree] Input stream not open, creating new tree\n";
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
            cout << "[BPlusTree] Failed to load valid tree, creating new\n";
            root = new Node(true);
        }
        else
        {
            cout << "[BPlusTree] Tree loaded successfully\n";
        }
    }
    catch (const exception &e)
    {
        cout << "[BPlusTree] Load failed: " << e.what() << ", creating new tree\n";
        clear();
        root = new Node(true);
    }
}

BPlusTree::Node *BPlusTree::loadNode(ifstream &in)
{
    if (!in.good())
    {
        cout << "[BPlusTree] Input stream not good\n";
        return nullptr;
    }
    bool isLeaf;
    in.read(reinterpret_cast<char *>(&isLeaf), sizeof(bool));
    if (!in.good())
    {
        cout << "[BPlusTree] Failed to read isLeaf\n";
        return nullptr;
    }
    Node *node = new Node(isLeaf);
    size_t keyCount;
    in.read(reinterpret_cast<char *>(&keyCount), sizeof(size_t));
    if (!in.good())
    {
        cout << "[BPlusTree] Failed to read keyCount\n";
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
            cout << "[BPlusTree] Failed to read keySize for key " << i << "\n";
            delete node;
            return nullptr;
        }
        char *keyBuffer = new char[keySize + 1];
        in.read(keyBuffer, keySize);
        if (!in.good())
        {
            cout << "[BPlusTree] Failed to read key data for key " << i << "\n";
            delete[] keyBuffer;
            delete node;
            return nullptr;
        }
        keyBuffer[keySize] = '\0';
        node->keys[i] = trim(keyBuffer);
        cout << "[BPlusTree] Loaded key: '" << node->keys[i] << "'\n";
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
                cout << "[BPlusTree] Failed to read value " << j << " for key " << i << "\n";
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
            cout << "[BPlusTree] Failed to read childCount\n";
            delete node;
            return nullptr;
        }
        node->children.resize(childCount);
        for (size_t i = 0; i < childCount; ++i)
        {
            node->children[i] = loadNode(in);
            if (!node->children[i])
            {
                cout << "[BPlusTree] Failed to load child " << i << "\n";
                delete node;
                return nullptr;
            }
        }
    }
    return node;
}

void BPlusTree::clear()
{
    delete root;
    root = nullptr;
}