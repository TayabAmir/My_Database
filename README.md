🗃️ Custom Database Management System (DBMS)
Overview
This is a custom-built, lightweight C++ Database Management System featuring:

SQL-like query support (e.g., FIND, KILL, CREATE, INSERT, UPDATE)

Support for joins, indexing with B+ Trees, foreign keys, data types, and constraints

Robust transaction management with rollback and commit

Demonstration on a sample company database

🔧 Compilation
Ensure you have a C++17-compatible compiler. Compile the system using:

bash
Copy
Edit
g++ main.cpp BPlusTree.cpp global.cpp Transaction.cpp db.cpp parser.cpp -o mydb.exe -std=c++17
▶️ Running
bash
Copy
Edit
./mydb.exe
📦 Features
✅ Core Functionality
Table Creation with column types and constraints (PRIMARY KEY, FOREIGN KEY, NOT NULL)

Index Creation on specific columns using B+ Trees

Data Insertion, Updation, Deletion, and Selection

Multi-table Joins with conditions

String and Integer comparison operators in WHERE clauses

Constraint Violation Handling and Error Reporting

🔄 Transaction System
Atomic Commit and Rollback

Safe state management using temporary log files

Integrity-preserving crash resilience for updates and deletions

🗂️ Example Usage
sql
Copy
Edit
CREATE DATABASE company;
USE company;

// All statements should be pasted as a single line

CREATE TABLE departments (
  dept_id INT PRIMARY KEY, 
  dept_name STRING(50) NOT_NULL, 
  location STRING(50)
);

CREATE TABLE employees (
  emp_id INT PRIMARY KEY,
  emp_name STRING(50) NOT_NULL,
  dept_id INT FOREIGN KEY REFERENCES departments(dept_id),
  salary INT,
  hire_date STRING(20)
);

INSERT INTO departments VALUES (1, "Engineering", "New York");
INSERT INTO employees VALUES (101, "Alice Smith", 1, 75000, "2023-01-15");

CREATE INDEX ON employees(dept_id);

FIND * FROM employees WHERE salary > 70000;
🧪 Advanced Test Cases

Update users set salary = 90000 where id = 2
kill from users where id = 2

Join conditions across 2–3 tables

Violation tests for NOT_NULL, PRIMARY KEY, and FOREIGN KEY

Updates using string and numerical comparisons

Deletion triggering foreign key constraints

🧠 Example Complex Query
sql
Copy
Edit
FIND * FROM employees 
JOIN projects ON employees.dept_id = projects.dept_id 
WHERE projects.budget > 300000 AND employees.salary >= 80000;
📁 Folder Structure
pgsql
Copy
Edit
.
├── main.cpp
├── db.cpp / db.hpp
├── BPlusTree.cpp / BPlusTree.hpp
├── Transaction.cpp / Transaction.hpp
├── global.cpp / global.hpp
├── parser.cpp / parser.hpp
└── README.md
