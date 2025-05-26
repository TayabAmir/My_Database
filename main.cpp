// main.cpp
#include <iostream>
#include "parser.hpp"
#include "acl.hpp"

using namespace std;

int main() {
    cout << "Custom C++ DBMS\n";
    ACL::getInstance().loadACL();

    string username, password;
    cout << "Enter username: ";
    getline(cin, username);
    cout << "Enter password: ";
    getline(cin, password);

    if (!ACL::getInstance().authenticate(username, password)) {
        cout << "Authentication failed. Exiting.\n";
        return 1;
    }

    cout << "Authenticated as " << username << "\nType your query:\n";
    string query;
    while (true) {
        cout << ">> ";
        getline(cin, query);
        if (query == "exit")
            break;
        handleQuery(query);
    }

    return 0;
}