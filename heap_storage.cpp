#include "heap_storage.h"
#include <cstring>
#include <stdexcept>
#include <string>
#include <db_cxx.h>
#include <vector>


typedef u_int16_t u16;

/**
 * SlottedPage implementation
 */
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

RecordID SlottedPage::add(const Dbt *data) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("Not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = static_cast<u16>(data->get_size());
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    std::memcpy(this->address(loc), data->get_data(), size);
    return id;
}

Dbt* SlottedPage::get(RecordID record_id) {
    u16 size, loc;
    this->get_header(size, loc, record_id);
    if (loc == 0) // Record has been deleted
        return nullptr;
    return new Dbt(this->address(loc), size);
}

void SlottedPage::put(RecordID record_id, const Dbt &data) {
    u16 size, loc;
    get_header(size, loc, record_id);
    u16 new_size = (u16)data.get_size();
    u16 extra = 0;
    if (new_size > size)
    {
        extra = new_size - size;
        if (!has_room(extra))
            throw DbBlockNoRoomError("not enough room for new record");
        // need to shift records to make room for new data
        slide(loc, loc - extra);
        memcpy(this->address(loc - extra), data.get_data(), new_size);
    }
    else
    {
        memcpy(this->address(loc), data.get_data(), new_size);
        slide(loc + new_size, loc + size);
    }

    put_header();
    put_header(record_id, new_size, loc);
}

void SlottedPage::del(RecordID record_id) {
    u16 size, loc;
    get_header(size, loc, record_id);
    put_header(record_id, 0, 0); // Mark the record as deleted
    slide(loc, loc + size);
}

RecordIDs* SlottedPage::ids(void) {
    u16 size, loc;
    RecordIDs *ids = new RecordIDs();
    for (RecordID i = 1; i <= this->num_records; i++)
    {
        get_header(size, loc, i);
        if (loc == 0) // if already deleted
            continue;
        ids->push_back(i);
    }
    return ids;
}

bool SlottedPage::has_room(u16 size) {
    // Calculate available space considering the new record header
    u16 required_space = size + sizeof(u16) * 2; // Size and location for header
    u16 available_space = this->end_free - (this->num_records * sizeof(u16) * 2 + sizeof(u16) * 2);
    return required_space <= available_space;
}

void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id) {
    size = get_n(4 * id);
    loc = get_n(4 * id + 2);
}

void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // Block header
        size = this->num_records;
        loc = this->end_free;
    }
    put_n((u16)4 * id, size);
    put_n((u16)4 * id + 2, loc);
}

void SlottedPage::slide(u_int16_t start, u_int16_t end) {
    // Calculate the amount to slide
    int slide_amount = end - start;
    if (slide_amount == 0) return; // No sliding needed

    memmove(this->address(this->end_free + slide_amount + 1), address(this->end_free + 1), start - (this->end_free + 1));

    RecordIDs *ids = this->ids();
    for (RecordID &id : *ids)
    {
        u16 size, loc;
        get_header(size, loc, id);
        if (loc <= start)
        {
            loc += slide_amount;
            put_header(id, size, loc);
        }
    }
    end_free += slide_amount;
    put_header();
}


u16 SlottedPage::get_n(u16 offset) {
    return *(u16 *)this->address(offset);
}

void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16 *)this->address(offset) = n;
}

void* SlottedPage::address(u16 offset) {
    void *addr = (void *)((char *)this->block.get_data() + offset);
    return addr;
}

/**
 * HeapFile implementation
 */
void HeapFile::create() {
    if (!closed) {
        throw std::runtime_error("File is already open");
    }
    this->db_open(DB_CREATE | DB_EXCL);
    this->closed = false;
    // Create an empty block to act as the first block
    SlottedPage* page = this->get_new();
}

void HeapFile::drop() {
    this->close();
    Db(_DB_ENV, 0).remove(this->dbfilename.c_str(), nullptr, 0);
    std::remove(this->dbfilename.c_str());
}

void HeapFile::open() {
    if (!closed) {
        return; // File is already open
    }
    this->db_open();
    this->closed = false;
}

void HeapFile::close() {
    if (closed) {
        return; // File is already closed
    }
    this->db.close(0);
    this->closed = true;
}

SlottedPage* HeapFile::get_new() {
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // Write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // Write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0); // Berkeley DB now manages the memory
    return page;
}

SlottedPage* HeapFile::get(BlockID block_id) {
    Dbt key(&block_id, sizeof(block_id));
    Dbt data;
    this->db.get(nullptr, &key, &data, 0); // Get the block from Berkeley DB
    return new SlottedPage(data, block_id, false);
}

void HeapFile::put(DbBlock* block) {
    int block_id = block->get_block_id(); // Store the block ID in a local variable
    Dbt key(&block_id, sizeof(block_id)); // Now you can take the address of block_id
    this->db.put(nullptr, &key, block->get_block(), 0); // Write the block back to the file
}

BlockIDs* HeapFile::block_ids() {
    BlockIDs *blockIds = new BlockIDs;
    for (BlockID i = 1; i <= (BlockID)this->last; i++)
    {
        blockIds->push_back(i);
    }
    return blockIds;
}

void HeapFile::db_open(uint flags) {
    try {
        if (!this->closed) {
            std::cerr << "Database already open" << std::endl;
            return; // Database is already open
        }

        // Attempt to open the database
        this->dbfilename = "example.db";
        this->db.set_re_len(DbBlock::BLOCK_SZ);
        db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, flags, 0644); // RECNO is a record number database, 0644 is unix file permission
        DB_BTREE_STAT *stat;
        this->db.stat(nullptr, &stat, DB_FAST_STAT);
        uint32_t bt_ndata = stat->bt_ndata;
        this->last = bt_ndata;
        this->closed = false;
    } catch (const std::exception& e) {
        std::cerr << "exception: " << e.what() << std::endl;
        throw;
    }
}

/**
 * HeapTable implementation
 */
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)
    : DbRelation(table_name, column_names, column_attributes), file(table_name){}

void HeapTable::create() {
    this->file.create();
}

void HeapTable::create_if_not_exists() {
    try {
        this->open(); // Attempt to open, which succeeds if the file exists
    } catch (const std::exception& e) {
        // If opening fails, assume the file does not exist and create it
        this->create();
    }
}

void HeapTable::drop() {
    this->file.drop();
}

void HeapTable::open() {
    this->file.open();
}

void HeapTable::close() {
    this->file.close();
}

Handle HeapTable::insert(const ValueDict *row) {
    // Directly use the append function to insert the new row
    this->open();
    return this->append(this->validate(row));
}

// Appends a new row to the table and returns a handle (block ID and record ID) to the newly inserted row.
Handle HeapTable::append(const ValueDict *row) {
    Dbt *data = marshal(row); // Convert the row into a Dbt object suitable for storage.
    SlottedPage *block = this->file.get(this->file.get_last_block_id()); // Retrieve the last block in the file.
    RecordID id;
    try {
        id = block->add(data); // Attempt to add the new data to the block.
    }
    catch (DbRelationError const &) {
        // If adding the data to the current block fails, allocate a new block and add the data there.
        block = this->file.get_new();
        id = block->add(data);
    }
    this->file.put(block); // Write the updated block back to the file.
    delete block; // Clean up the SlottedPage object.
    delete[] (char *)data->get_data(); // Clean up the allocated memory for the data.
    delete data; // Clean up the Dbt object.
    return Handle(this->file.get_last_block_id(), id); // Return a handle to the newly inserted row.
}


Handles* HeapTable::select(const ValueDict *where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = this->file.block_ids();
    for (auto const& block_id : *block_ids) {
        SlottedPage* block = this->file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id : *record_ids) {
            handles->push_back(std::make_pair(block_id, record_id));
        }
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

void HeapTable::update(const Handle handle, const ValueDict *new_values) {
    // Example implementation logic:
    // 1. Fetch the record using the handle.
    // 2. Apply the changes from new_values to the record.
    // 3. Marshal the updated record and write it back to the storage.

    // This is a placeholder.
}

void HeapTable::del(const Handle handle) {
    // Example implementation logic:
    // 1. Locate the record using the handle.
    // 2. Mark the record as deleted in the storage system.

    // This is a placeholder. 
}

ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names) {
    BlockID blockID = handle.first;
    RecordID recordID = handle.second;
    SlottedPage *block = this->file.get(blockID);
    Dbt *data = block->get(recordID);
    ValueDict *rows = unmarshal(data);
    if (column_names == nullptr || column_names->empty())
        return rows;

    ValueDict *filteredRows = new ValueDict;
    for (const auto &colName : *column_names)
    {
        auto it = rows->find(colName);
        if (it != rows->end())
        {
            (*filteredRows)[colName] = it->second;
        }
    }
    delete rows;
    delete block;
    delete data;
    return filteredRows;
}

ValueDict* HeapTable::project(Handle handle) {
    return project(handle, &this->column_names); // Delegate to the more specific project function using the table's column names.
}

// Validates the input row against the table schema to ensure all required columns are present.
// Returns a complete row dictionary with all column names from the table schema.
ValueDict *HeapTable::validate(const ValueDict *row) {
    ValueDict *fullRow = new ValueDict(); // Create a new row dictionary to hold validated data.

    for (const auto &column_name : this->column_names) { // Iterate over all column names defined in the table schema.
        auto it = row->find(column_name); // Attempt to find each column name in the input row.

        if (it == row->end()) { // If a column name is missing in the input row, clean up and throw an error.
            delete fullRow;
            throw DbRelationError("Column '" + column_name + "' is missing in the row.");
        }
        (*fullRow)[column_name] = it->second; // Copy the column's value from the input row to the validated row.
    }
    return fullRow; // Return the validated row with all required columns present.
}


// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

ValueDict* HeapTable::unmarshal(Dbt* data) {
    char* bytes = static_cast<char*>(data->get_data());
    ValueDict* row = new ValueDict();
    uint offset = 0;
    uint col_num = 0;

    for (const auto& column_name : this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            int32_t val = *(reinterpret_cast<int32_t*>(bytes + offset));
            offset += sizeof(int32_t);
            (*row)[column_name] = Value(val);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = *(reinterpret_cast<u16*>(bytes + offset));
            offset += sizeof(u16);
            std::string val(bytes + offset, size);
            offset += size;
            (*row)[column_name] = Value(val);
        } else {
            throw DbRelationError("Unknown data type while unmarshaling");
        }
    }
    return row;
}

Handles *HeapTable::select() {
    // Logic to iterate over all records and add their handles to the list
    return select(nullptr);
}

// test function -- returns true if all tests pass
bool test_heap_storage() {
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles* handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
    	return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
		return false;
    table.drop();

    return true;
}



