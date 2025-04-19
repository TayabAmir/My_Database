#pragma once
#include <iostream>
#include <vector>
#include <algorithm>
#include <memory>

template <typename KeyType>
class BTree
{
private:
    struct Node
    {
        std::vector<KeyType> keys;
        std::vector<Node *> children;
        bool isLeaf;

        Node(bool leaf = true);
    };

    Node *root;
    int degree;

public:
    BTree(int degree = 3);
    void insert(const KeyType &key);
    bool search(const KeyType &key);
    void remove(const KeyType &key);

private:
    void split(Node *parent, int index);
    void insertNonFull(Node *node, const KeyType &key);
    bool searchNode(Node *node, const KeyType &key);
    void removeFromNode(Node *node, const KeyType &key);
};

template <typename KeyType>
BTree<KeyType>::Node::Node(bool leaf) : isLeaf(leaf) {}

template <typename KeyType>
BTree<KeyType>::BTree(int degree) : degree(degree)
{
    root = new Node();
}

template <typename KeyType>
void BTree<KeyType>::insert(const KeyType &key)
{
    if (root->keys.size() == 2 * degree - 1)
    {
        Node *newRoot = new Node(false);
        newRoot->children.push_back(root);
        split(newRoot, 0);
        root = newRoot;
    }
    insertNonFull(root, key);
}

template <typename KeyType>
bool BTree<KeyType>::search(const KeyType &key)
{
    return searchNode(root, key);
}

template <typename KeyType>
void BTree<KeyType>::split(Node *parent, int index)
{
    Node *fullNode = parent->children[index];
    Node *newNode = new Node(fullNode->isLeaf);
    parent->children.insert(parent->children.begin() + index + 1, newNode);
    int mid = degree - 1;
    parent->keys.insert(parent->keys.begin() + index, fullNode->keys[mid]);
    newNode->keys = std::vector<KeyType>(fullNode->keys.begin() + mid + 1, fullNode->keys.end());
    fullNode->keys.resize(mid);
    if (!fullNode->isLeaf)
    {
        newNode->children = std::vector<Node *>(fullNode->children.begin() + mid + 1, fullNode->children.end());
        fullNode->children.resize(mid + 1);
    }
}

template <typename KeyType>
void BTree<KeyType>::insertNonFull(Node *node, const KeyType &key)
{
    if (node->isLeaf)
    {
        node->keys.push_back(key);
        std::sort(node->keys.begin(), node->keys.end());
    }
    else
    {
        int i = node->keys.size() - 1;
        while (i >= 0 && key < node->keys[i])
        {
            --i;
        }
        ++i;

        if (node->children[i]->keys.size() == 2 * degree - 1)
        {
            split(node, i);
            if (key > node->keys[i])
            {
                ++i;
            }
        }
        insertNonFull(node->children[i], key);
    }
}

template <typename KeyType>
bool BTree<KeyType>::searchNode(Node *node, const KeyType &key)
{
    int i = 0;
    while (i < node->keys.size() && key > node->keys[i])
    {
        ++i;
    }

    if (i < node->keys.size() && node->keys[i] == key)
    {
        return true;
    }

    if (node->isLeaf)
    {
        return false;
    }

    return searchNode(node->children[i], key);
}

template <typename KeyType>
void BTree<KeyType>::remove(const KeyType &key)
{
    if (!root)
        return;
    removeFromNode(root, key);
}

template <typename KeyType>
void BTree<KeyType>::removeFromNode(Node *node, const KeyType &key)
{
    if (node->isLeaf)
    {
        auto it = std::find(node->keys.begin(), node->keys.end(), key);
        if (it != node->keys.end())
        {
            node->keys.erase(it);
        }
    }
    else
    {
        std::cerr << "BTree: Deletion for internal nodes not implemented yet.\n";
    }
}
