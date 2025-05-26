// acl.cpp
#include "acl.hpp"
#include <fstream>
#include <sstream>
#include <filesystem>
#include <iostream>
#include "global.hpp"
using namespace std;

ACL* ACL::instance = nullptr;

void ACL::addUser(const string& username, const string& password, const string& role) {
    if (users.find(username) != users.end()) {
        throw runtime_error("User '" + username + "' already exists.");
    }
    if (roles.find(role) == roles.end()) {
        throw runtime_error("Role '" + role + "' does not exist.");
    }
    users[username] = {username, password, role};
    saveACL();
}

bool ACL::authenticate(const string& username, const string& password) {
    auto it = users.find(username);
    if (it != users.end() && it->second.password == password) {
        setCurrentUser(username);
        return true;
    }
    return false;
}

bool ACL::hasPermission(const string& username, aclOperation op, const string& dbName, const string& tableName) {
    if (users.find(username) == users.end()) {
        return false;
    }
    const string& roleName = users[username].role;
    const Role& role = roles[roleName];

    if(role.name == "admin") return true;

    // Check if operation is allowed
    if (role.allowedOperations.find(op) == role.allowedOperations.end()) {
        return false;
    }

    for(auto &d : role.allowedDatabases) cout << d << " ";
    cout << endl;

    for(auto &d : role.allowedTables) cout << d << " ";
    cout << endl;

    // Check database access (if applicable)
    if (!dbName.empty() && !role.allowedDatabases.empty() && role.allowedDatabases.find(dbName) == role.allowedDatabases.end()) {
        return false;
    }

    // Check table access (if applicable)
    if (!tableName.empty() && !role.allowedTables.empty() && role.allowedTables.find(tableName) == role.allowedTables.end()) {
        return false;
    }

    return true;
}

void ACL::addRole(const string& roleName, const set<aclOperation>& ops, const set<string>& dbs, const set<string>& tables) {
    roles[roleName] = {roleName, ops, dbs, tables};
    saveACL();
}

void ACL::saveACL() {
    string aclPath = "databases/system/acl.data";
    filesystem::create_directories("databases/system");
    ofstream out(aclPath);
    if (!out) {
        throw runtime_error("Failed to save ACL data.");
    }

    // Save roles
    for (const auto& [name, role] : roles) {
        out << "ROLE " << name;
        for (const auto& op : role.allowedOperations) {
            out << " " << static_cast<int>(op);
        }
        out << " DBS:";
        for (const auto& db : role.allowedDatabases) {
            out << db << ",";
        }
        out << " TABLES:";
        for (const auto& table : role.allowedTables) {
            out << table << ",";
        }
        out << "\n";
    }

    // Save users
    for (const auto& [username, user] : users) {
        out << "USER " << username << " " << user.password << " " << user.role << "\n";
    }
    out.close();
}

void ACL::loadACL() {
    string aclPath = "databases/system/acl.data";
    ifstream in(aclPath);
    if (!in) {
        // Initialize default admin role and user
        addRole("admin", {
            aclOperation::CREATE_DATABASE, aclOperation::CREATE_TABLE, aclOperation::INSERT,
            aclOperation::SELECT, aclOperation::UPDATE, aclOperation::DELETE,
            aclOperation::DESCRIBE, aclOperation::SHOW_TABLES
        });
        addUser("admin", "admin123", "admin");
        return;
    }

    string line;
    while (getline(in, line)) {
        istringstream iss(line);
        string type;
        iss >> type;

        if (type == "ROLE") {
            string roleName;
            iss >> roleName;
            set<aclOperation> ops;
            int op;
            while (iss >> op) {
                if (iss.peek() == 'D') break; // Reached "DBS:"
                ops.insert(static_cast<aclOperation>(op));
            }
            set<string> dbs, tables;
            string token;
            iss >> token; // "DBS:"
            getline(iss, token, ':'); // Get databases until "TABLES:"
            istringstream dbStream(token);
            string db;
            while (getline(dbStream, db, ',')) {
                if (!db.empty()) dbs.insert(db);
            }
            getline(iss, token); // Get tables
            istringstream tableStream(token);
            string table;
            while (getline(tableStream, table, ',')) {
                if (!table.empty()) tables.insert(table);
            }
            roles[roleName] = {roleName, ops, dbs, tables};
        }
        else if (type == "USER") {
            string username, password, role;
            iss >> username >> password >> role;
            users[username] = {username, password, role};
        }
    }
    in.close();
}