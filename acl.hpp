// acl.hpp
#ifndef ACL_HPP
#define ACL_HPP

#include <string>
#include <vector>
#include <map>
#include <set>

using namespace std;

enum class aclOperation {
    CREATE_DATABASE, CREATE_TABLE, INSERT, SELECT, UPDATE, DELETE, DESCRIBE, SHOW_TABLES, USE_DATABASE
};

struct User {
    string username;
    string password; // In production, use hashed passwords
    string role;
};

struct Role {
    string name;
    set<aclOperation> allowedOperations;
    set<string> allowedDatabases; // Empty means all databases
    set<string> allowedTables;    // Empty means all tables in allowed databases
};

class ACL {
private:
    static ACL* instance;
    map<string, User> users; // username -> User
    map<string, Role> roles; // roleName -> Role
    string currentUser;

    ACL() {}

public:
    static ACL& getInstance() {
        if (!instance)
            instance = new ACL();
        return *instance;
    }

    void addUser(const string& username, const string& password, const string& role);
    bool authenticate(const string& username, const string& password);
    bool hasPermission(const string& username, aclOperation op, const string& dbName = "", const string& tableName = "");
    void setCurrentUser(const string& username) { currentUser = username; }
    string getCurrentUser() const { return currentUser; }
    void addRole(const string& roleName, const set<aclOperation>& ops, const set<string>& dbs = {}, const set<string>& tables = {});
    void saveACL();
    void loadACL();
};

#endif