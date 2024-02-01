# 5300-Dolphin 
Student DB Relation Manager project for CPSC5300 at Seattle U, Winter 2024

## Installation

```
# Clone the repository
git clone [https://github.com/yourusername/projectname.git](https://github.com/klundeen/5300-Dolphin.git)

# Navigate to the project directory
cd 5300-Dolphin

# Compile the project
make

# Then, navigate to the src directory
cd src
```

## Usage

```
# Run the spl shell by executing ./sql5300 (path to DB directory)
./sql5300 ~/cpsc5300/5300-Dolphin/data/

# Running SQL will not execute it but instead returns a string result of parsing the query
SQL> SELECT * FROM student
Parsed successfully!
SELECT * FROM student

# Alternatively, you could type "test" to run customized tests. Currently, the program tests functionalities on heap table
# database name is hard-coded as "example.db". You could change it in heap_storage.cpp if you wish
create ok
drop ok
create_if_not_exists ok
try insert
insert ok
select ok 1
project ok
ok
```

## Project Structure

```
5300-Dolphin/
├── src/
│   ├── storage_engine.h // header file for SlottedPage, HeapTable, and HeapFile
│   ├── sql5300.cpp // driver class implementation. Includes sqlshell, DBEnv, and SQLParser class
│   └── heap_storage.cpp // Implementation for heap_storage engine classes
├── data/ // This is where the database is stored
├── README.md
├── Makefile
└── LICENSE
```
