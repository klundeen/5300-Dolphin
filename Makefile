# Makefile, Kevin Lundeen, Seattle University, CPSC5300, Winter Quarter 2024
CXX      ?= g++
CPPFLAGS  = -I/usr/local/db6/include -I$(INC_DIR) -Wall -Wextra -Wpedantic
CXXFLAGS  = -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O2 -std=c++17
LDFLAGS  += -L/usr/local/db6/lib
LDLIBS    = -ldb_cxx -lsqlparser

SRC_DIR  := src
INC_DIR  := include
OBJ_DIR  := obj

# following is a list of all the compiled object files needed to build the sql5300 executable
SRCS := $(wildcard $(SRC_DIR)/*.cpp)
OBJS := $(subst $(SRC_DIR),$(OBJ_DIR),$(SRCS:.cpp=.o))

.PHONY: all
all: sql5300

# Rule for linking to create the executable
# Note that this is the default target since it is the first non-generic one in the Makefile: $ make
sql5300: $(OBJS)
	$(CXX) $(LDFLAGS) $^ $(LDLIBS) -o $@

# In addition to the general .cpp to .o rule below, we need to note any header dependencies here
# idea here is that if any of the included header files changes, we have to recompile
HEAP_STORAGE_H = $(addprefix $(INC_DIR)/,heap_storage.h SlottedPage.h HeapFile.h HeapTable.h storage_engine.h)
SCHEMA_TABLES_H = $(INC_DIR)/schema_tables.h $(HEAP_STORAGE_H)
SQLEXEC_H = $(INC_DIR)/SQLExec.h $(SCHEMA_TABLES_H)
$(OBJ_DIR)/ParseTreeToString.o : $(INC_DIR)/ParseTreeToString.h
$(OBJ_DIR)/SQLExec.o : $(SQLEXEC_H)
$(OBJ_DIR)/SlottedPage.o : $(INC_DIR)/SlottedPage.h
$(OBJ_DIR)/HeapFile.o : $(INC_DIR)/HeapFile.h $(INC_DIR)/SlottedPage.h
$(OBJ_DIR)/HeapTable.o : $(HEAP_STORAGE_H)
$(OBJ_DIR)/schema_tables.o : $(SCHEMA_TABLES_) $(INC_DIR)/ParseTreeToString.h
$(OBJ_DIR)/sql5300.o : $(SQLEXEC_H) $(INC_DIR)/ParseTreeToString.h
$(OBJ_DIR)/storage_engine.o : $(INC_DIR)/storage_engine.h

# General rules for compilation
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@

# Rule for removing all non-source files (so they can get rebuilt from scratch)
# Note that since it is not the first target, you have to invoke it explicitly: $ make clean
.PHONY: clean
clean:
	$(RM) sql5300 $(OBJ_DIR)/*.o

