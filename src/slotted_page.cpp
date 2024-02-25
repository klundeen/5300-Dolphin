/**
 * @file slotted_page.cpp
 * @author K Lundeen
 * @see Seattle University, CPSC5300
 */
#include "slotted_page.h"
#include <cstring>

using namespace std;
typedef uint16_t u16;

/**
 * SlottedPage constructor
 * @param block
 * @param block_id
 * @param is_new
 */
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new)
    : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

/**
 * Add a new record to the block.
 * @param data
 * @return the new block's id
 */
RecordID SlottedPage::add(const Dbt *data) {
    if (!has_room((u16)data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16)data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1U;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

/**
 * Get a record from the block.
 * @param record_id
 * @return the bits of the record as stored in the block, or nullptr if it has
 * been deleted (freed by caller)
 */
Dbt *SlottedPage::get(RecordID record_id) const {
    u16 size, loc;
    get_header(size, loc, record_id);
    if (loc == 0)
        return nullptr; // this is just a tombstone, record has been deleted
    return new Dbt(this->address(loc), size);
}

/**
 * Replace the record with the given data.
 * @param record_id   record to replace
 * @param data        new contents of record_id
 * @throws DbBlockNoRoomError if it won't fit
 */
void SlottedPage::put(RecordID record_id, const Dbt &data) {
    u16 size, loc;
    get_header(size, loc, record_id);
    u16 new_size = (u16)data.get_size();
    if (new_size > size) {
        u16 extra = new_size - size;
        if (!has_room(extra))
            throw DbBlockNoRoomError("not enough room for enlarged record");
        slide(loc, loc - extra);
        memcpy(this->address(loc - extra), data.get_data(), new_size);
    } else {
        memcpy(this->address(loc), data.get_data(), new_size);
        slide(loc + new_size, loc + size);
    }
    get_header(size, loc, record_id);
    put_header(record_id, new_size, loc);
}

/**
 * Delete a record from the page.
 *
 * Mark the given id as deleted by changing its size to zero and its location to
 * 0. Compact the rest of the data in the block. But keep the record ids the
 * same for everyone.
 *
 * @param record_id  record to delete
 */
void SlottedPage::del(RecordID record_id) {
    u16 size, loc;
    get_header(size, loc, record_id);
    put_header(record_id, 0, 0); // 0 is the tombstone sentinel
    slide(loc, loc + size);
}

/**
 * Sequence of all non-deleted record IDs.
 * @return  sequence of IDs (freed by caller)
 */
RecordIDs *SlottedPage::ids(void) const {
    RecordIDs *vec = new RecordIDs();
    u16 size, loc;
    for (RecordID record_id = 1; record_id <= this->num_records; record_id++) {
        get_header(size, loc, record_id);
        if (loc != 0)
            vec->push_back(record_id);
    }
    return vec;
}

/**
 * Get the size and offset for given id. For id of zero, it is the block header.
 * @param size  set to the size from given header
 * @param loc   set to the byte offset from given header
 * @param id    the id of the header to fetch
 */
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc,
                             RecordID id) const {
    size = get_n((u16)4 * id);
    loc = get_n((u16)(4 * id + 2));
}

/**
 * Store the size and offset for given id. For id of zero, store the block
 * header.
 * @param id
 * @param size
 * @param loc
 */
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id ==
        0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n((u16)4 * id, size);
    put_n((u16)(4 * id + 2), loc);
}

/**
 * Calculate if we have room to store a record with given size. The size should
 * include the 4 bytes for the header, too, if this is an add.
 * @param size   size of the new record (not including the header space needed)
 * @return       true if there is enough room, false otherwise
 */
bool SlottedPage::has_room(u16 size) const {
    u16 headers = (u16)(4 * (this->num_records + 1));
    u16 unused;
    if (this->end_free <= headers)
        unused = 0;
    else
        unused = this->end_free - headers;
    return size + (u16)4 <= unused;
}

/**
 * Slide the contents to compensate for a smaller/larger record.
 *
 * If start < end, then remove data from offset start up to but not including
 * offset end by sliding data that is to the left of start to the right. If
 * start > end, then make room for extra data from end to start by sliding data
 * that is to the left of start to the left. Also fix up any record headers
 * whose data has slid. Assumes there is enough room if it is a left shift (end
 * < start).
 *
 * @param start  beginning of slide
 * @param end    end of slide
 */
void SlottedPage::slide(u_int16_t start, u_int16_t end) {
    int shift = end - start;
    if (shift == 0)
        return;

    // slide data
    void *to = this->address((u16)(this->end_free + 1 + shift));
    void *from = this->address((u16)(this->end_free + 1));
    int bytes = start - (this->end_free + 1U);
    memmove(to, from, bytes);

    // fix up headers to the right
    RecordIDs *record_ids = ids();
    for (auto const &record_id : *record_ids) {
        u16 size, loc;
        get_header(size, loc, record_id);
        if (loc <= start) {
            loc += shift;
            put_header(record_id, size, loc);
        }
    }
    delete record_ids;
    this->end_free += shift;
    put_header();
}

/**
 * Get 2-byte integer at given offset in block.
 */
u16 SlottedPage::get_n(u16 offset) const {
    return *(u16 *)this->address(offset);
}

/**
 * Put a 2-byte integer at given offset in block.
 * @param offset number of bytes into the page
 * @param n
 */
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16 *)this->address(offset) = n;
}

/**
 * Make a void* pointer for a given offset into the data block.
 * @param offset
 * @return
 */
void *SlottedPage::address(u16 offset) const {
    return (void *)((char *)this->block.get_data() + offset);
}
