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

All the example test queries are combined and ran.

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
test_btree: splitting leaf 2, new sibling 3 starting at value 211
new root: (interior block 4): 2|211|3
splitting leaf 3, new sibling 5 starting at value 324
splitting leaf 5, new sibling 6 starting at value 437
splitting leaf 6, new sibling 7 starting at value 550
splitting leaf 7, new sibling 8 starting at value 663
splitting leaf 8, new sibling 9 starting at value 776
splitting leaf 9, new sibling 10 starting at value 889
splitting leaf 10, new sibling 11 starting at value 1002
splitting leaf 11, new sibling 12 starting at value 1115
splitting leaf 12, new sibling 13 starting at value 1228
splitting leaf 13, new sibling 14 starting at value 1341
splitting leaf 14, new sibling 15 starting at value 1454
...
...
...
splitting leaf 881, new sibling 882 starting at value 98747
splitting leaf 882, new sibling 883 starting at value 98860
splitting leaf 883, new sibling 884 starting at value 98973
splitting leaf 884, new sibling 885 starting at value 99086
splitting leaf 885, new sibling 886 starting at value 99199
splitting leaf 886, new sibling 887 starting at value 99312
splitting leaf 887, new sibling 888 starting at value 99425
splitting leaf 888, new sibling 889 starting at value 99538
splitting leaf 889, new sibling 890 starting at value 99651
splitting leaf 890, new sibling 891 starting at value 99764
splitting leaf 891, new sibling 892 starting at value 99877
first lookup passed
second lookup passed
third lookup passed
all lookups passed
ok

SQL> show tables;create table foo (id int, data text);show tables;show columns from foo;create index fx on foo (id);create index fz on foo (data);show index from foo;insert into foo (id, data) values (1,"one");select * from foo;insert into foo values (2, "Two"); insert into foo values (3, "Three"); insert into foo values (99, "wowzers, Penny!!");select * from foo;select * from foo where id=3;select * from foo where id=1 and data="one";select * from foo where id=99 and data="nine";select id from foo;select data from foo where id=1;delete from foo where id=1;select * from foo;delete from foo;insert into foo values (2, "Two"); insert into foo values (3, "Three"); insert into foo values (99, "wowzers, Penny!!");select * from foo;drop index fz from foo;show index from foo;insert into foo (id) VALUES (100);select * from foo;drop table foo;show tables;create table foo (id int, data text);insert into foo values (1,"one");insert into foo values(2,"two"); insert into foo values (2, "another two");select * from foo;create index fxx on foo (id);show index from foo;delete from foo where data = "two";select * from foo;create index fxx on foo (id);show index from foo;insert into foo values (4,"four");select * from foo;

SHOW TABLES
table_name 
+----------+
successfully returned 0 rows
CREATE TABLE foo (id INT, data TEXT)
created foo
SHOW TABLES
table_name 
+----------+
"foo" 
successfully returned 1 rows
SHOW COLUMNS FROM foo
table_name column_name data_type 
+----------+----------+----------+
"foo" "id" "INT" 
"foo" "data" "TEXT" 
successfully returned 2 rows
CREATE INDEX fx ON foo USING BTREE (id)
created index fx
CREATE INDEX fz ON foo USING BTREE (data)
created index fz
SHOW INDEX
table_name index_name column_name seq_in_index index_type is_unique 
+----------+----------+----------+----------+----------+----------+
"foo" "fx" "id" 1 "BTREE" true 
"foo" "fz" "data" 1 "BTREE" true 
successfully returned 2 rows
INSERT INTO foo (id, data) VALUES (1, "one")
successfully inserted 1 row into foo and 2 indices
SELECT * FROM foo
id data 
+----------+----------+
1 "one" 
successfully returned 1 rows
INSERT INTO foo VALUES (2, "Two")
successfully inserted 1 row into foo and 2 indices
INSERT INTO foo VALUES (3, "Three")
successfully inserted 1 row into foo and 2 indices
INSERT INTO foo VALUES (99, "wowzers, Penny!!")
successfully inserted 1 row into foo and 2 indices
SELECT * FROM foo
id data 
+----------+----------+
1 "one" 
2 "Two" 
3 "Three" 
99 "wowzers, Penny!!" 
successfully returned 4 rows
SELECT * FROM foo WHERE id = 3
id data 
+----------+----------+
3 "Three" 
successfully returned 1 rows
SELECT * FROM foo WHERE id = 1 AND data = "one"
id data 
+----------+----------+
1 "one" 
successfully returned 1 rows
SELECT * FROM foo WHERE id = 99 AND data = "nine"
id data 
+----------+----------+
successfully returned 0 rows
SELECT id FROM foo
id 
+----------+
1 
2 
3 
99 
successfully returned 4 rows
SELECT data FROM foo WHERE id = 1
data 
+----------+
"one" 
successfully returned 1 rows
DELETE FROM foo WHERE id = 1
Error: DbRelationError: duplicate index foo fz
SELECT * FROM foo
id data 
+----------+----------+
2 "Two" 
3 "Three" 
99 "wowzers, Penny!!" 
successfully returned 3 rows
DELETE FROM foo
successfully deleted 3 rows from foo and 1 indices
INSERT INTO foo VALUES (2, "Two")
successfully inserted 1 row into foo and 1 indices
INSERT INTO foo VALUES (3, "Three")
successfully inserted 1 row into foo and 1 indices
INSERT INTO foo VALUES (99, "wowzers, Penny!!")
successfully inserted 1 row into foo and 1 indices
SELECT * FROM foo
id data 
+----------+----------+
2 "Two" 
3 "Three" 
99 "wowzers, Penny!!" 
successfully returned 3 rows
DROP INDEX fz FROM foo
dropped index fz
SHOW INDEX
table_name index_name column_name seq_in_index index_type is_unique 
+----------+----------+----------+----------+----------+----------+
successfully returned 0 rows
INSERT INTO foo (id) VALUES (100)
Error: DbRelationError: don't know how to handle NULLs, defaults, etc. yet
SELECT * FROM foo
id data 
+----------+----------+
2 "Two" 
3 "Three" 
99 "wowzers, Penny!!" 
successfully returned 3 rows
DROP TABLE foo
dropped foo
SHOW TABLES
table_name 
+----------+
successfully returned 0 rows
CREATE TABLE foo (id INT, data TEXT)
created foo
INSERT INTO foo VALUES (1, "one")
successfully inserted 1 row into foo
INSERT INTO foo VALUES (2, "two")
successfully inserted 1 row into foo
INSERT INTO foo VALUES (2, "another two")
successfully inserted 1 row into foo
SELECT * FROM foo
id data 
+----------+----------+
1 "one" 
2 "two" 
2 "another two" 
successfully returned 3 rows
CREATE INDEX fxx ON foo USING BTREE (id)
Error: DbRelationError: Duplicate keys are not allowed in unique index
SHOW INDEX
table_name index_name column_name seq_in_index index_type is_unique 
+----------+----------+----------+----------+----------+----------+
successfully returned 0 rows
DELETE FROM foo WHERE data = "two"
successfully deleted 1 rows from foo
SELECT * FROM foo
id data 
+----------+----------+
1 "one" 
2 "another two" 
successfully returned 2 rows
CREATE INDEX fxx ON foo USING BTREE (id)
created index fxx
SHOW INDEX
table_name index_name column_name seq_in_index index_type is_unique 
+----------+----------+----------+----------+----------+----------+
"foo" "fxx" "id" 1 "BTREE" true 
successfully returned 1 rows
INSERT INTO foo VALUES (4, "four")
successfully inserted 1 row into foo and 1 indices
SELECT * FROM foo
id data 
+----------+----------+
1 "one" 
2 "another two" 
4 "four" 
successfully returned 3 rows
```


## Project Structure

```
5300-Dolphin/
├── include/
│   ├── heap_file.h // Definition for HeapFile, a heap implementation of DbFile
│   ├── heap_table.h // Definition for HeapFile, a heap implementation of DbRelation
│   ├── ParseTreeToString.h // Class that converts a Hyrise AST to string
│   ├── sql_exec.h // Execute a Hyrise AST with SQLExec and return a QueryResult
│   ├── slotted_page.h // Definition for SlottedPage, a heap implementation of DbBlock
│   ├── heap_storage.h // header file that includes slotted_page.h, heap_table.h, and heap_file.h
│   ├── schema_tables.h // Definition of schema tables like _tables and _columns
│   └── storage_engine.h // Definition of the ADTs DbBlock, DbFile, and DbRelation
├── obj/ // Build directory
├── src/
│   ├── heap_file.cpp // Implementation of HeapFile
│   ├── heap_table.cpp // Implementation of HeapTable
│   ├── ParseTreeToString.cpp // Implementation of ParseTreeToString
│   ├── sql_exec.cpp // Implementation of SQLExec
│   ├── slotted_page.cpp // Implementation of SlottedPage
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
- `Milestone5`: Implements INSERT, DELETE, SELECT with WHERE condition
- `Milestone6`: Implements lookup to support BtreeIndex operations 
