CCFLAGS     = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -ggdb
COURSE      = /usr/local/db6
INCLUDE_DIR = $(COURSE)/include
LIB_DIR     = $(COURSE)/lib

OBJ_DIR     = ./obj
INC_DIR	 	= ./include
SRC_DIR	 	= ./src

FILES = \
slotted_page heap_file heap_table \
sql_exec schema_tables heap_storage \
storage_engine ParseTreeToString

HDRS 			= $(addsuffix .h, $(FILES))
OBJS 			= $(addsuffix .o, $(FILES)) sql5300.o
HDRS_PATH  		= $(addprefix $(INC_DIR)/, $(HDRS))
OBJS_PATH  		= $(addprefix $(OBJ_DIR)/, $(OBJS))

sql5300: mkdir_build $(OBJS_PATH)
	g++ -L$(LIB_DIR) -o $@ $(OBJS_PATH) -ldb_cxx -lsqlparser
 
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp $(HDRS_PATH)
	g++ -I$(INCLUDE_DIR) -I$(INC_DIR) $(CCFLAGS) -o $@ $<
 
mkdir_build:
	mkdir -p $(OBJ_DIR)
 
clean:
	$(RM) sql5300 $(OBJ_DIR)/*.o
