#include "heap_storage.h"
#include <cstring>

using namespace std;
/**
 * Test helper. Sets the row's a and b values.
 * @param row to set
 * @param a column value
 * @param b column value
 */
void test_set_row(ValueDict &row, int a, string b) {
    row["a"] = Value(a);
    row["b"] = Value(b);
    row["c"] = Value(a % 2 == 0); // true for even, false for od
}

/**
 * Test helper. Compares row to expected values for columns a and b.
 * @param table    relation where row is
 * @param handle   row to check
 * @param a        expected column value
 * @param b        expected column value
 * @return         true if actual == expected for both columns, false otherwise
 */
bool test_compare(DbRelation &table, Handle handle, int a, string b) {
    ValueDict *result = table.project(handle);
    Value value = (*result)["a"];
    if (value.n != a) {
        delete result;
        return false;
    }
    value = (*result)["b"];
    if (value.s != b) {
        delete result;
        return false;
    }
    value = (*result)["c"];
    delete result;
    if (value.n != (a % 2 == 0))
        return false;
    return true;
}

/**
 * Testing function for heap storage engine.
 * @return true if the tests all succeeded
 */
bool test_heap_storage() {
    if (!test_slotted_page())
        return assertion_failure("slotted page tests failed");
    cout << endl << "slotted page tests ok" << endl;

    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    column_names.push_back("c");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::BOOLEAN);
    column_attributes.push_back(ca);

    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    cout << "create ok" << endl;
    table1.drop(); // drop makes the object unusable because of BerkeleyDB
                   // restriction -- maybe want to fix this some day
    cout << "drop ok" << endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    cout << "create_if_not_exists ok" << endl;

    ValueDict row;
    string b = "Four score and seven years ago our fathers brought forth on "
               "this continent, a new nation, conceived in Liberty, and "
               "dedicated to the proposition that all men are created equal.";
    test_set_row(row, -1, b);
    table.insert(&row);
    cout << "insert ok" << endl;
    Handles *handles = table.select();
    if (!test_compare(table, (*handles)[0], -1, b))
        return false;
    cout << "select/project ok " << handles->size() << endl;
    delete handles;

    Handle last_handle;
    for (int i = 0; i < 1000; i++) {
        test_set_row(row, i, b);
        last_handle = table.insert(&row);
    }
    handles = table.select();
    if (handles->size() != 1001)
        return false;
    int i = -1;
    for (auto const &handle : *handles) {
        if (!test_compare(table, handle, i++, b))
            return false;
    }
    cout << "many inserts/select/projects ok" << endl;
    delete handles;

    table.del(last_handle);
    handles = table.select();
    if (handles->size() != 1000)
        return false;
    i = -1;
    for (auto const &handle : *handles) {
        if (!test_compare(table, handle, i++, b))
            return false;
    }
    cout << "del ok" << endl;
    table.drop();
    delete handles;
    return true;
}


/**
 * Print out given failure message and return false.
 * @param message reason for failure
 * @return false
 */
bool assertion_failure(string message, double x, double y) {
    cout << "FAILED TEST: " << message;
    if (x >= 0)
        cout << " " << x;
    if (y >= 0)
        cout << " " << y;
    cout << endl;
    return false;
}

/**
 * Testing function for SlottedPage.
 * @return true if testing succeeded, false otherwise
 */
bool test_slotted_page() {
    // construct one
    char blank_space[DbBlock::BLOCK_SZ];
    Dbt block_dbt(blank_space, sizeof(blank_space));
    SlottedPage slot(block_dbt, 1, true);

    // add a record
    char rec1[] = "hello";
    Dbt rec1_dbt(rec1, sizeof(rec1));
    RecordID id = slot.add(&rec1_dbt);
    if (id != 1)
        return assertion_failure("add id 1");

    // get it back
    Dbt *get_dbt = slot.get(id);
    string expected(rec1, sizeof(rec1));
    string actual((char *)get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 1 back " + actual);

    // add another record and fetch it back
    char rec2[] = "goodbye";
    Dbt rec2_dbt(rec2, sizeof(rec2));
    id = slot.add(&rec2_dbt);
    if (id != 2)
        return assertion_failure("add id 2");

    // get it back
    get_dbt = slot.get(id);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *)get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 2 back " + actual);

    // test put with expansion (and slide and ids)
    char rec1_rev[] = "something much bigger";
    rec1_dbt = Dbt(rec1_rev, sizeof(rec1_rev));
    slot.put(1, rec1_dbt);
    // check both rec2 and rec1 after expanding put
    get_dbt = slot.get(2);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *)get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 2 back after expanding put of 1 " +
                                 actual);
    get_dbt = slot.get(1);
    expected = string(rec1_rev, sizeof(rec1_rev));
    actual = string((char *)get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 1 back after expanding put of 1 " +
                                 actual);

    // test put with contraction (and slide and ids)
    rec1_dbt = Dbt(rec1, sizeof(rec1));
    slot.put(1, rec1_dbt);
    // check both rec2 and rec1 after contracting put
    get_dbt = slot.get(2);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *)get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 2 back after contracting put of 1 " +
                                 actual);
    get_dbt = slot.get(1);
    expected = string(rec1, sizeof(rec1));
    actual = string((char *)get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 1 back after contracting put of 1 " +
                                 actual);

    // test del (and ids)
    RecordIDs *id_list = slot.ids();
    if (id_list->size() != 2 || id_list->at(0) != 1 || id_list->at(1) != 2)
        return assertion_failure("ids() with 2 records");
    delete id_list;
    slot.del(1);
    id_list = slot.ids();
    if (id_list->size() != 1 || id_list->at(0) != 2)
        return assertion_failure("ids() with 1 record remaining");
    delete id_list;
    get_dbt = slot.get(1);
    if (get_dbt != nullptr)
        return assertion_failure("get of deleted record was not null");

    // try adding something too big
    rec2_dbt = Dbt(
        nullptr, DbBlock::BLOCK_SZ -
                     10); // too big, but only because we have a record in there
    try {
        slot.add(&rec2_dbt);
        return assertion_failure("failed to throw when add too big");
    } catch (const DbBlockNoRoomError &exc) {
        // test succeeded - this is the expected path
    } catch (...) {
        // Note that this won't catch segfault signals -- but in that case we
        // also know the test failed
        return assertion_failure("wrong type thrown when add too big");
    }

    // more volume
    string gettysburg =
        "Four score and seven years ago our fathers brought forth on this "
        "continent, a new nation, conceived in Liberty, and dedicated to the "
        "proposition that all men are created equal.";
    int32_t n = -1;
    uint16_t text_length = gettysburg.size();
    uint total_size = sizeof(n) + sizeof(text_length) + text_length;
    char *data = new char[total_size];
    *(int32_t *)data = n;
    *(uint16_t *)(data + sizeof(n)) = text_length;
    memcpy(data + sizeof(n) + sizeof(text_length), gettysburg.c_str(),
           text_length);
    Dbt dbt(data, total_size);
    vector<SlottedPage> page_list;
    BlockID block_id = 1;
    Dbt slot_dbt(new char[DbBlock::BLOCK_SZ], DbBlock::BLOCK_SZ);
    slot = SlottedPage(slot_dbt, block_id++, true);
    for (int i = 0; i < 10000; i++) {
        try {
            slot.add(&dbt);
        } catch (DbBlockNoRoomError &exc) {
            page_list.push_back(slot);
            slot_dbt = Dbt(new char[DbBlock::BLOCK_SZ], DbBlock::BLOCK_SZ);
            slot = SlottedPage(slot_dbt, block_id++, true);
            slot.add(&dbt);
        }
    }
    page_list.push_back(slot);
    for (const auto &slot : page_list) {
        RecordIDs *ids = slot.ids();
        for (RecordID id : *ids) {
            Dbt *record = slot.get(id);
            if (record->get_size() != total_size)
                return assertion_failure("more volume wrong size", block_id - 1,
                                         id);
            void *stored = record->get_data();
            if (memcmp(stored, data, total_size) != 0)
                return assertion_failure("more volume wrong data", block_id - 1,
                                         id);
            delete record;
        }
        delete ids;
        delete[] (char *)slot.block
            .get_data(); // this is why we need to be a friend--just convenient
    }
    delete[] data;
    return true;
}
