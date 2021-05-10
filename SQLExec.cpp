/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2021"
 * Sprint Otono: Shrividya Ballapadavu, Priyanka patil
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;
Indices *SQLExec::indices = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

//destructor deletes references to column_names,column_attributes & rows created
QueryResult::~QueryResult() {
    if (column_names != nullptr)
        delete column_names;
    if (column_attributes != nullptr)
        delete column_attributes;
    if (rows != nullptr) {
        for (auto row:*rows) {
            delete row;
        }
        delete rows;
    }

}


QueryResult *SQLExec::execute(const SQLStatement *statement) {

    //initializes _tables table, if not yet present
    if (SQLExec::tables == nullptr)
        SQLExec::tables = new Tables();
    if (SQLExec::indices == nullptr)
        SQLExec::indices = new Indices();

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

//Sets column_attribute's data type based on col's type
void
SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {

    column_name = col->name;
    switch (col->type) {
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        default:
            throw SQLExecError("Unrecognized Data Type");
    }

}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch (statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
    }
}


/**
 * Creates the table based on the statement
 * @param statement : CreateStatement
 * @return QueryResult
 */
QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    Identifier table_name = statement->tableName;
    ColumnNames column_names;
    ColumnAttributes column_attributes;

    Identifier column_name;
    ColumnAttribute columnAttribute;

    //add column_names & identify columnAttributes
    for (ColumnDefinition *col: *statement->columns) {
        column_definition(col, column_name, columnAttribute);
        column_names.push_back(column_name);
        column_attributes.push_back(columnAttribute);
    }

    //update _tables schema
    ValueDict row;
    row["table_name"] = table_name;
    Handle t_handle = SQLExec::tables->insert(&row);

    try {
        //update _columns schema
        Handles c_handles;
        DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(
                        column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                c_handles.push_back(columns.insert(&row));  // Insert into _columns
            }

            //create the relation
            DbRelation &table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists) {
                table.create_if_not_exists();
            } else {
                table.create();
            }
        } catch (exception &e) {
            //attempt to undo the insertions into _columns
            try {
                for (auto const &handle:c_handles) {
                    columns.del(handle);
                }
            } catch (...) {}
            throw;
        }

    } catch (exception &e) {
        //attempt to undo the insertion into _tables
        try {
            SQLExec::tables->del(t_handle);
        } catch (...) {}
        throw;
    }

    return new QueryResult(std::string("Created ") + table_name);
}

/**
 * Creates the index based on the statement
 * @param statement : CreateStatement
 * @return QueryResult
 */
QueryResult *SQLExec::create_index(const CreateStatement *statement) {

    Identifier table_name = statement->tableName;
    Identifier index_name = statement->indexName;
    Identifier index_type;

    Identifier column_name;
    ColumnNames column_names;

    try {
        index_type = statement->indexType;
    } catch (exception &e) {
        index_type = "BTREE"; //either BTREE OR HASH
    }


    //Execute the statement
    ValueDict row;
    row["table_name"] = table_name;
    row["index_name"] = index_name;
    row["seq_in_index"] = 0;
    row["index_type"] = index_type;
    row["is_unique"] = Value(std::string(statement->indexType) == "BTREE"); // assume HASH is non-unique --

    for (auto const &col_name: *statement->indexColumns) {
        column_names.push_back(col_name);
    }

    Handles i_handles;
    for (auto const col_name:column_names) {
        row["column_name"] = col_name;
        int seq_in_index = row["seq_in_index"].n;
        row["seq_in_index"] = seq_in_index + 1;
        i_handles.push_back(SQLExec::indices->insert(&row));

    }

    DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
    index.create(); //create the index
    return new QueryResult(std::string("Created index ") + index_name);
}


// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("Only DROP TABLE and DROP INDEX are implemented");
    }
}


/**
 * Drop the given table.
 * @param statement: table to drop
 * @return QueryResult: result of the drop query
 */
QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    Identifier table_name = statement->name;

    if (statement->type != DropStatement::kTable) {
        return new QueryResult("Unrecognized DROP type");
    }

    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME) {
        throw SQLExecError("Cannot drop a schema table!");
    }
    ValueDict where;
    where["table_name"] = Value(table_name);

    // remove indices
    IndexNames index_names = SQLExec::indices->get_index_names(table_name);
    if (!index_names.empty()) {
        try {

            Handles *i_handles = SQLExec::indices->select(&where);
            SQLExec::indices->del(*i_handles->begin());
            delete i_handles;

            for (Identifier index_name:index_names) {
                DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
                index.drop();
            }
        } catch (exception &e) {
            throw;
        }

    }

    // get the table
    DbRelation &table = SQLExec::tables->get_table(table_name);

    //remove from _column schema
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles *c_handles = columns.select(&where);

    for (auto const &handle: *c_handles) {
        columns.del(handle);
    }
    delete c_handles;

    //remove table
    table.drop();

    //remove from _table schema
    c_handles = SQLExec::tables->select(&where);
    SQLExec::tables->del(*c_handles->begin());
    delete c_handles;

    return new QueryResult("Dropped " + table_name);
}

/**
 * Drop the given index.
 * @param statement: index to drop
 * @return QueryResult: result of the drop index query
 */
QueryResult *SQLExec::drop_index(const DropStatement *statement) {

    Identifier table_name = statement->name;
    Identifier index_name = statement->indexName;

    ValueDict where;
    where["table_name"] = Value(table_name);
    where["index_name"] = Value(index_name);

    DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
    index.drop();
    Handles *handles = SQLExec::indices->select(&where);

    for (auto const &handle: *handles) {
        SQLExec::indices->del(handle);
    }


    return new QueryResult("Dropped index " + index_name);
}

/**
 * Helper method to call required Show routine based on statement type
 * @param statement of type ShowStatement
 * @return QueryResult
 */

QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
            break;
        case ShowStatement::kColumns:
            return show_columns(statement);
            break;
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError("Unrecognized show type!");
    }
}

/**
 * Show index of the given table.
 * @param statement: columns to show
 * @return QueryResult: result of the show columns query
 */

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    Identifier table_name = statement->tableName;
    ColumnAttributes *column_attributes = new ColumnAttributes;

    ValueDict where;
    where["table_name"] = Value(table_name);

    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("index_name");
    column_names->push_back("column_name");
    column_names->push_back("seq_in_index");
    column_names->push_back("index_type");
    column_names->push_back("is_unique");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    //create handles for tables
    Handles *handles = SQLExec::indices->select(&where);

    ValueDicts *rows = new ValueDicts;

    //get the table names
    for (const auto &handle:*handles) {
        ValueDict *row = SQLExec::indices->project(handle, column_names);
        Identifier table_name = row->at("index_name").s;

        //check if the table not present in Table's or Column's TABLE_NAME
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME) {
            rows->push_back(row);
        } else {
            delete row;
        }
    }



    string ret("successfully returned ");
    ret += to_string(rows->size());
    ret += " rows";

    delete handles;

    return new QueryResult(column_names, column_attributes, rows, ret);


}

/**
 * Show all existing tables
 * @return QueryResult: result of show table
 */
QueryResult *SQLExec::show_tables() {

    ColumnNames *column_names = new ColumnNames;
    ColumnAttributes *column_attributes = new ColumnAttributes;

    //add "table_name" with type
    column_names->push_back("table_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    //create handles for tables
    Handles *handles = SQLExec::tables->select();


    ValueDicts *rows = new ValueDicts;

    //get the table names
    for (const auto &handle:*handles) {
        ValueDict *row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;

        //check if the table not present in Table's or Column's TABLE_NAME
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME && table_name != Indices::TABLE_NAME) {
            rows->push_back(row);
        } else {
            delete row;
        }

    }
    u_long n = handles->size() - 2;

    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");


}

/**
 * Show all current columns.
 * @param statement: columns to show
 * @return QueryResult: result of the show columns query
 */
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    // SHOW COLUMNS FROM <table>

    Identifier table_name = statement->tableName;

    ValueDict where;
    where["table_name"] = Value(table_name);


    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles *c_handles = columns.select(&where);

    ColumnNames *column_names = new ColumnNames();
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    ColumnAttributes *column_attributes = new ColumnAttributes();
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDicts *rows = new ValueDicts;

    for (auto const &handle: *c_handles) {
        ValueDict *aRow = columns.project(handle, column_names);
        rows->push_back(aRow);
    }
    delete c_handles;

    string ret("successfully returned ");
    ret += to_string(rows->size());
    ret += " rows";

    return new QueryResult(column_names, column_attributes, rows, ret);
}