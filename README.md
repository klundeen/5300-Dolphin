# 5300-Dolphin
![dolphin](/assets/dolphin.gif)
<br>

Sprint 1: Hailey Dice and Nathan Nishi
## Handoff Information for Sprint 1
### See handoff video under Assets for download
We believe we have provided all of the necessary code. All methods are commented with relevant information and documented. We implemented them to the best of our ability, however, due to time constraints, the code still has a few errors when attempting to compile. Our best attempt was made at clearing as many errors as possible. Contact Nathan or Hailey if there are any questions. Best of luck on Sprint 2.

## Sprint 1: Milestone 1 Skeleton
Found under the tag Milestone1, this milestone "assembles all of our tools and build the core of our SQL shell". Input SQL statements will parse, and be the corresponding parse tree will be printed. 
### Getting Started
Clone the repo on a Seattle U cs1 server

```bash
$ cd ~/cpsc5300
$ git clone https://github.com/klundeen/5300-Dolphin.git
$ cd 5300-Dolphin
$ git checkout tags/Milestone1
$ make
```
Run the program with 
```bash 
$ ./sql5300 ~/cpsc5300/data
```

### Requirements
* **Berkley DB** in your directory, on CS1. Used as ```#include "db_cxx.h"```
* **SQL Parser** updated in your ```~/.bash_profile```

See more [here](https://seattleu.instructure.com/courses/1597073/pages/getting-set-up-on-cs1) on Canvas

## Sprint 1: Milestone 2 Rudimentary Storage Engine
Found under the Milestone2 tag, this milestone begins to create a relation manager by creating the basis of the heap storage engine, which uses a slotted page structure.<br> ![fig13](/assets/fig13.png)
<br><br>This storage engine will have three layers:
1. **DbBlock:** how records are stored and retrieved within the block
2. **DbFile:** that handles the collection of blocks that make up the relation, and also the creation, deletion and access to them. Uses BerkleyDB buffer manager for reading/writing to disk
3. **DbRelation:** presents the logical view of the table

Each layer corresponds to a concrete class:
1. **SlottedPage**
2. **HeapFile**
3. **HeapTable**

### Getting Started
In the directory where ```https://github.com/klundeen/5300-Dolphin.git``` is cloned
```bash
$ git checkout tags/Milestone2
$ make
```

