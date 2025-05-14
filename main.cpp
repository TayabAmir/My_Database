#include <iostream>
#include <string>
#include "parser.hpp"

using namespace std;

int main()
{
    cout << "=============================\n";
    cout << "   Custom C++ SQL-like DBMS  \n";
    cout << "=============================\n";
    cout << "Type your SQL query below.\n";
    cout << "Type 'exit' to quit the program.\n\n";

    string query;
    while (true)
    {
        string currentDb = Context::getInstance().getCurrentDatabase();
        cout << "[" << (currentDb.empty() ? "no-db" : currentDb) << "] > ";
        getline(cin, query);

        // Trim whitespace
        while (!query.empty() && isspace(query.back()))
            query.pop_back();

        if (query == "exit")
        {
            cout << "\nExiting DBMS. Goodbye!\n";
            break;
        }

        if (query.empty())
        {
            continue;
        }

        handleQuery(query);
    }

    return 0;
}
