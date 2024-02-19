# 5300-Dolphin 

Student DB Relation Manager project for CPSC5300 at Seattle U, Winter 2024

## Usage

``` sh
./sql5300 dbenvpath
```

Options:

- `dbenvpath`: Path to database

### Example

``` sh
$ ./sql5300 $(mktemp -d)
(sql5300: running with database environment at /tmp/tmp.QqH0e90pCw)
SQL> select * from foo where foo.x = 10
SELECT * FROM foo WHERE foo.x = 10
not implemented
SQL> show tables
SHOW TABLES
table_name
+----------+
successfully returned 0 rows
SQL> show columns from _tables
SHOW COLUMNS FROM _tables
table_name column_name data_type
+----------+----------+----------+
"_tables" "table_name" "TEXT"
successfully returned 1 rows
SQL> show columns from _columns
SHOW COLUMNS FROM _columns
table_name column_name data_type
+----------+----------+----------+
"_columns" "table_name" "TEXT"
"_columns" "column_name" "TEXT"
"_columns" "data_type" "TEXT"
successfully returned 3 rows
SQL> create table foo (id int, data text, x integer, y integer, z integer)
CREATE TABLE foo (id INT, data TEXT, x INT, y INT, z INT)
created foo
SQL> create table foo (goober int)
CREATE TABLE foo (goober INT)
Error: DbRelationError: foo already exists
SQL> create table goo (x int, x text)
CREATE TABLE goo (x INT, x TEXT)
Error: DbRelationError: duplicate column goo.x
SQL> show tables
SHOW TABLES
table_name
+----------+
"foo"
successfully returned 1 rows
SQL> show columns from foo
SHOW COLUMNS FROM foo
table_name column_name data_type
+----------+----------+----------+
"foo" "id" "INT"
"foo" "data" "TEXT"
"foo" "x" "INT"
"foo" "y" "INT"
"foo" "z" "INT"
successfully returned 5 rows
SQL> drop table foo
DROP TABLE foo
dropped foo
SQL> show tables
SHOW TABLES
table_name
+----------+
successfully returned 0 rows
SQL> show columns from foo
SHOW COLUMNS FROM foo
table_name column_name data_type
+----------+----------+----------+
successfully returned 0 rows
SQL> quit
```

## Set Up <a name="setup"></a>

### Dependencies

Building requires the following libraries:

- [SQL Parser for Hyrise (fork)](https://github.com/klundeen/sql-parser)
- [Berkeley DB](https://www.oracle.com/database/technologies/related/berkeleydb.html) (tested on 6.2.32, 18.1.38)

### Building <a name="building"></a>

Building can be accomplished by running gnumake

``` sh
make
```

### Testing

The SlottedPage, HeapFile, and HeapTable classes can be tested with:

``` sh
$ ./sql5300 $(mktemp -d)
(sql5300: running with database environment at /tmp/tmp.khR2WMVuuq)
SQL> test
test_heap_storage:
slotted page tests ok
create ok
drop ok
create_if_not_exists ok
insert ok
select/project ok 1
many inserts/select/projects ok
del ok
ok
SQL>
```


## Project Structure

```
5300-Dolphin/
├── include/
│   ├── HeapFile.h // Definition for HeapFile, a heap implementation of DbFile
│   ├── HeapTable.h // Definition for HeapFile, a heap implementation of DbRelation
│   ├── ParseTreeToString.h // Class that converts a Hyrise AST to string
│   ├── SQLExec.h // Execute a Hyrise AST with SQLExec and return a QueryResult
│   ├── SlottedPage.h // Definition for SlottedPage, a heap implementation of DbBlock
│   ├── heap_storage.h // header file that includes SlottedPage.h, HeapTable.h, and HeapFile.h
│   ├── schema_tables.h // Definition of schema tables like _tables and _columns
│   └── storage_engine.h // Definition of the ADTs DbBlock, DbFile, and DbRelation
├── obj/ // Build directory
├── src/
│   ├── HeapFile.cpp // Implementation of HeapFile
│   ├── HeapTable.cpp // Implementation of HeapTable
│   ├── ParseTreeToString.cpp // Implementation of ParseTreeToString
│   ├── SQLExec.cpp // Implementation of SQLExec
│   ├── SlottedPage.cpp // Implementation of SlottedPage
│   ├── schema_tables.cpp // Implementation of Tables and Columns
│   ├── sql5300.cpp // driver class implementation. Includes setup for DbEnv and a SQL shell
│   └── storage_engine.cpp // Implementation of equality functions for the ADT DB classes
├── README.md
├── Makefile
└── LICENSE
```

## Tags

- `Milestone1`: Initial parser support that simply prints back parsed SQL syntax
- `Milestone2`: Adds Heap storage classes and related tests
- `Milestone3_prep`: Refactors and adds support for schema _tables and _columns
- `Milestone3`: Implements CREATE TABLE, DROP TABLE, SHOW TABLES, and SHOW COLUMNS
- `Milestone4`: Implements CREATE INDEX, DROP INDEX, and SHOW INDEX
