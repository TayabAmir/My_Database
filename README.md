# 🗃️ Custom Database Management System (DBMS)

## Overview

This is a **custom-built, lightweight C++ Database Management System** featuring:

- SQL-like query support (`FIND`, `KILL`, `CREATE`, `INSERT`, `UPDATE`)
- Support for **joins**, **B+ Tree indexing**, **foreign keys**, **data types**, and **constraints**
- In-memory and file-based data operations
- Robust **transaction management** with rollback and commit
- Demonstration using a sample `company` database

---

## 🔧 Compilation

Ensure a C++17-compatible compiler is installed. Compile the system using:

```bash
g++ main.cpp BPlusTree.cpp global.cpp Transaction.cpp db.cpp parser.cpp -o mydb.exe -std=c++17

## Running
./mydb.exe

## Example Queries
USE company;
CREATE TABLE departments (dept_id INT PRIMARY KEY, dept_name STRING(50) NOT_NULL, location STRING(50));
CREATE TABLE employees (emp_id INT PRIMARY KEY, emp_name STRING(50) NOT_NULL, dept_id INT FOREIGN KEY REFERENCES departments(dept_id), salary INT, hire_date STRING(20));
CREATE TABLE projects (proj_id INT PRIMARY KEY, proj_name STRING(50) NOT_NULL, dept_id INT FOREIGN KEY REFERENCES departments(dept_id), budget INT);
CREATE TABLE assignments (assign_id INT PRIMARY KEY, emp_id INT FOREIGN KEY REFERENCES employees(emp_id), proj_id INT FOREIGN KEY REFERENCES projects(proj_id), hours INT);
CREATE INDEX ON employees(dept_id);
CREATE INDEX ON projects(dept_id);
CREATE INDEX ON employees(emp_id);
CREATE INDEX ON projects(proj_id);

# Insert
INSERT INTO departments VALUES (1, "Engineering", "New York");
INSERT INTO departments VALUES (2, "HR", "Chicago");
INSERT INTO departments VALUES (3, "Marketing", "San Francisco");
INSERT INTO departments VALUES (4, "Finance", "Boston");

# Search
FIND * FROM employees WHERE dept_id <= 5 AND salary > 75000

# Update
UPDATE employees SET bonus = 1000 WHERE emp_id = 101

# Delete
KILL FROM assignments WHERE emp_id = 101 AND hours > 35 OR proj_id = 201

# Transaction
use Begin to start
use commit to end
