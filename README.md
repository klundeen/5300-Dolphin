# 5300-Dolphin

<br>

Sprint 2: Shrividya Ballapadavu and Priyanka Patil

Handoff video link: https://youtu.be/y7eQDc03lFw

## Sprint 2: Milestone 3: Schema Storage 

### Instructions to run Milestone 3 on CS1 

1. Clone the repo on a Seattle U cs1 server

Follow the follwing commands. First clone the 5300-Dolphin from inside 
cpsc5300 directory, go inside the 5300-Dolphin directory,
checkout to Milestone3 tag. Then run the make command and finally run the sql5300.cpp is using ./sql5300 ~/cpsc5300/data. Now you can run the commands for creating, dropping, displaying tables and columns.


```bash
$ cd ~/cpsc5300
$ git clone https://github.com/klundeen/5300-Dolphin.git
$ cd 5300-Dolphin
$ git checkout tags/Milestone3 
$ make
```
2. Run the program with 
```bash 
$ ./sql5300 ~/cpsc5300/data
```

## Milestone 4:Indexing Setup

### Instructions to run Milestone4 on CS1 

1. From inside the 5300-Dolphin directory, follow the instructions
   to checkout to tag Milestone4, and run the command make,
   Finally run sql5300.cpp using ./sql5300 ~/cpsc5300/data command.
   sql> will show up where you can run the commands for creating ,displaying
   and dropping the index from a table.

```bash
$ git checkout tags/Milestone4
$ make
```
2. Run the program with 
```bash 
$ ./sql5300 ~/cpsc5300/data
```
</br>

# Sprint Invierno
Updated by Tuan Phan, Yinying Liang

To clone the repository: git clone https://github.com/klundeen/5300-Dolphin.git 

To make the program: type 'make'

To clean the make files: type 'make clean'

To run the program: ./sql5300 ~/cpsc5300/data (second arg is the path where your database env locates)

To exit the program: type 'quit'

## Milestone 5: Insert, Delete, Select, Simple Queries
Modified file: SQLExec.cpp

Checkout tag Milestone5: git checkout tags/Milestone5

Implemented methods: Insert, delete, select All and select based on predicates.

Milestone 5 program compiles and works expectedly

## Milestone 6: BTree Index
Modified file: btree.cpp

Checkout tab Milestone6: git checkout tags/Milestone6

Implemented methods: lookup, _lookup (recursive lookup)

Milestone 6 program compiles and works expectedly
