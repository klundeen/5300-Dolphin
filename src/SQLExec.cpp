/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter Quarter 2024"
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name : *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row : *qres.rows) {
            for (auto const &column_name : *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                case ColumnAttribute::INT:
                    out << value.n;
                    break;
                case ColumnAttribute::TEXT:
                    out << "\"" << value.s << "\"";
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

QueryResult::~QueryResult() {
    // FIXME
}

QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr) {
        SQLExec::tables = new Tables();
        SQLExec::tables->open();
    }

    try {
        switch (statement->type()) {
        case kStmtCreate:
            return create((const CreateStatement *)statement);
        case kStmtDrop:
            return drop((const DropStatement *)statement);
        case kStmtShow:
            return show((const ShowStatement *)statement);
        default:
            return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

void SQLExec::close() {
    if (SQLExec::tables != nullptr) {
        SQLExec::tables->clear_cache();
        SQLExec::tables->close();
        delete SQLExec::tables;
        SQLExec::tables = nullptr;
    }
}

void SQLExec::column_definition(const ColumnDefinition *col,
                                Identifier &column_name,
                                ColumnAttribute &column_attribute) {
    column_name = std::string(col->name);
    switch (col->type) {
    case ColumnDefinition::INT:
        column_attribute.set_data_type(ColumnAttribute::INT);
        break;
    case ColumnDefinition::TEXT:
        column_attribute.set_data_type(ColumnAttribute::TEXT);
        break;
    default:
        throw SQLExecError("Unknown column type");
    }
}

QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    // Create the table
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    for (ColumnDefinition *col : *statement->columns) {
        Identifier column_name;
        ColumnAttribute column_attribute;
        column_definition(col, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    // Add the table to the _tables table
    ValueDict row;
    Handle table_handle;
    try {
        row["table_name"] = Value(statement->tableName);
        table_handle = SQLExec::tables->insert(&row);
    } catch (const DbRelationError &e) {
        throw e;
    }

    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    // Add the columns to the _columns table
    Handles column_handles; // Track column handles so we can roll-back
    try {
        for (size_t i = 0; i < column_names.size(); i++) {
            row["table_name"] = Value(statement->tableName);
            row["column_name"] = Value(column_names[i]);
            switch (column_attributes[i].get_data_type()) {
            case ColumnAttribute::INT:
                row["data_type"] = Value("INT");
                break;
            case ColumnAttribute::TEXT:
                row["data_type"] = Value("TEXT");
                break;
            default:
                throw SQLExecError("Unknown column type");
            }
            Handle column_handle = columns.insert(&row);
            column_handles.push_back(column_handle);
        }
    } catch (const DbRelationError &e) {
        for (const Handle &handle : column_handles)
            columns.del(handle);
        SQLExec::tables->del(table_handle);
        throw e;
    }

    DbRelation &table = SQLExec::tables->get_table(statement->tableName);
    table.create(); // Creates the file for the table
    return new QueryResult("created " + std::string(statement->tableName));
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch (statement->type) {
    case CreateStatement::kTable:
        return create_table(statement);
        break;
    default:
        return new QueryResult("not implemented");
    }
}

QueryResult *SQLExec::drop_table(const std::string table_name) {
    // Check that the table exists and get its handle
    ValueDict where;
    where["table_name"] = Value(table_name);
    Handles *handles = SQLExec::tables->select(&where);
    if (handles->size() == 0) {
        delete handles;
        throw SQLExecError(table_name + " not found in _tables");
    }
    Handle handle = handles->at(0);
    delete handles;

    // Drop the table
    DbRelation &table = SQLExec::tables->get_table(table_name);
    table.drop();

    // Remove the table from the _tables table
    SQLExec::tables->del(handle);

    // Remove the columns from the _columns table
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    where.clear();
    where["table_name"] = Value(table_name);
    handles = columns.select(&where);
    for (Handle handle : *handles)
        columns.del(handle);
    delete handles;

    return new QueryResult("dropped " + table_name);
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
        case DropStatement::kTable:
            return drop_table(std::string(statement->name));
        default:
            return new QueryResult("not implemented");
    }
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
    return new QueryResult("not implemented"); // FIXME
}

QueryResult *SQLExec::show_tables() {
    return new QueryResult("not implemented"); // FIXME
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    return new QueryResult("not implemented"); // FIXME
}
