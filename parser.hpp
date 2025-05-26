#pragma once
#include <string>
#include "db.hpp"

void handleQuery(const std::string &query);
void handleInsert(const std::string &query);
void handleSelect(const std::string &query);
void handleUpdate(const std::string &query);
void handleDelete(const std::string &query);
bool validateForeignKey(const string &refTable, const string &refColumn, const string &dbName);
void handleCreateRole(const string& query);
void handleCreateUser(const string& query);