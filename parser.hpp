#pragma once
#include <string>
#include "db.hpp"

void handleQuery(const std::string &query);
void handleInsert(const std::string &query);
void handleSelect(const std::string &query);
void handleUpdate(const string &query);
void handleDelete(const string &query);
