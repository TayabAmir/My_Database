#include <iostream>
#include "parser.hpp"

using namespace std;

int main()
{
    cout << "Custom C++ DBMS\nType your query:\n";
    string query;
    while (true)
    {
        cout << ">> ";
        getline(cin, query);
        if (query == "exit")
            break;
        handleQuery(query);
    }

    return 0;
}
