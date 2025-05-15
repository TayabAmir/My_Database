#ifndef BPLUSTREE_HPP
#define BPLUSTREE_HPP

#include <vector>
#include <string>
#include <cstdint>
#include <vector>
#include <fstream>
using namespace std;

class BPlusTree
{
private:
    static const int ORDER = 4;
    struct Node
    {
        std::vector<std::string> keys;
        std::vector<std::vector<uint64_t>> values;
        std::vector<Node *> children;
        bool isLeaf;
        Node *next;
        Node(bool leaf = true);
        ~Node();
    };
    Node *root;
    std::string colType;

    int compareKeys(const std::string &a, const std::string &b);
    void splitChild(Node *parent, int index, Node *child);
    void insertNonFull(Node *node, const std::string &key, uint64_t offset);
    void saveNode(Node *node, std::ofstream &out);
    Node *loadNode(std::ifstream &in);
    void collectKeys(Node *node, vector<string> &keys);

public:
    BPlusTree(const std::string &type);
    ~BPlusTree();
    void getAllKeys(vector<string> &keys);
    std::vector<uint64_t> search(const std::string &key);
    void insert(const std::string &key, uint64_t offset);
    void save(std::ofstream &out);
    void load(std::ifstream &in);
    void clear();
};

std::string trim(const std::string &str);

#endif