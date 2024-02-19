/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter Quarter 2024"
 */
#include "SQLExec.h"
#include <sstream>

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;
Indices *SQLExec::indices = nullptr;

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

QueryResult::~QueryResult() {
    if (column_names != nullptr)
        delete column_names;
    if (column_attributes != nullptr)
        delete column_attributes;
    if (rows != nullptr) {
        for (auto row : *rows)
            delete row;
        delete rows;
    }
}

QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr) {
        SQLExec::tables = new Tables();
        SQLExec::tables->open();
    }
    // Open the _indices table
    if (SQLExec::indices == nullptr) {
        SQLExec::indices = new Indices();
        SQLExec::indices->open();
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
    if (SQLExec::indices != nullptr) {
        SQLExec::indices->clear_cache();
        SQLExec::indices->close();
        delete SQLExec::indices;
        SQLExec::indices = nullptr;
    }
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

QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    ValueDict row;
    row["table_name"] = Value(statement->tableName);
    row["index_name"] = Value(statement->indexName);
    row["seq_in_index"] = Value(0);
    row["index_type"] = Value(statement->indexType);
    row["is_unique"] = std::string(statement->indexType) == "BTREE"
                           ? Value(true)
                           : Value(false);

    Handles column_handles;
    try {
        for (const char *col : *statement->indexColumns) {
            row["column_name"] = Value(col);
            row["seq_in_index"].n++;
            Handle column_handle = SQLExec::indices->insert(&row);
            column_handles.push_back(column_handle);
        }
    } catch (const DbRelationError &e) {
        for (const Handle &handle : column_handles)
            SQLExec::indices->del(handle);
        throw e;
    }

    DbIndex &index =
        SQLExec::indices->get_index(statement->indexName, statement->tableName);
    index.create();
    return new QueryResult("created index " +
                           std::string(statement->indexName));
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch (statement->type) {
    case CreateStatement::kTable:
        return create_table(statement);
    case CreateStatement::kIndex:
        return create_index(statement);
    default:
        return new QueryResult("not implemented");
    }
}

QueryResult *SQLExec::drop_table(const std::string table_name) {
    std::stringstream result;

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

    // Drop indices
    for (Identifier const &index_name :
         SQLExec::indices->get_index_names(table_name)) {
        try {
            QueryResult *index_result = drop_index(table_name, index_name);
            result << *index_result << "\n";
            delete index_result;
        } catch (const SQLExecError &e) {
            // Warn but continue
            result << e.what() << "\n";
        }
    }

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

    result << "dropped " << table_name;
    return new QueryResult(result.str());
}

QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
    case DropStatement::kTable:
        return drop_table(std::string(statement->name));
    case DropStatement::kIndex:
        return drop_index(std::string(statement->name),
                          std::string(statement->indexName));
    default:
        return new QueryResult("not implemented");
    }
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
    if (statement->type == ShowStatement::kTables) {
        return show_tables();
    } else if (statement->type == ShowStatement::kColumns) {
        return show_columns(statement);
    } else {
        return new QueryResult("not implemented");
    }
}

QueryResult *SQLExec::show_tables() {
    ColumnNames *cn = new ColumnNames;
    ColumnAttributes *ca = new ColumnAttributes();
    ValueDicts *rows = new ValueDicts();
    Handles *handles = SQLExec::tables->select();
    std::string message;

    for (auto const &handle : *handles) {
        ValueDict *row = SQLExec::tables->project(handle, cn);
        Identifier table_name = row->at("table_name").s;
        Identifier table_table_name = Tables::TABLE_NAME;
        Identifier columns_table_name = Columns::TABLE_NAME;

        if (table_name != table_table_name &&
            table_name != columns_table_name) {
            rows->push_back(row);
        }
    }
    tables->get_columns(Tables::TABLE_NAME, *cn, *ca);
    message = "successfully returned " + std::to_string(rows->size()) + " rows";
    return new QueryResult(cn, ca, rows, message);
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    ColumnNames *cn = new ColumnNames();
    ColumnAttributes *ca = new ColumnAttributes();
    ValueDicts *rows = new ValueDicts();
    std::string message;

    cn->push_back("table_name");
    cn->push_back("column_name");
    cn->push_back("data_type");

    ColumnAttribute column_attr(ColumnAttribute::DataType::TEXT);
    ca->push_back(column_attr);

    ValueDict new_row;
    new_row["table_name"] = Value(statement->tableName);

    Identifier tab_name = Columns::TABLE_NAME;
    DbRelation &new_table = SQLExec::tables->get_table(tab_name);
    Handles *handles = new_table.select(&new_row);

    for (auto const &handle : *handles) {
        ValueDict *row = new_table.project(handle, cn);
        rows->push_back(row);
    }

    message = "successfully returned " + std::to_string(rows->size()) + " rows";
    return new QueryResult(cn, ca, rows, message);
}

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
     return new QueryResult("show index not implemented"); // FIXME
}

QueryResult *SQLExec::drop_index(const Identifier &table_name,
                                 const Identifier &index_name) {
    // Check that the index exists and get its handle
    ValueDict where;
    where["table_name"] = Value(table_name);
    where["index_name"] = Value(index_name);
    Handles *handles = SQLExec::indices->select(&where);
    if (handles->size() == 0) {
        delete handles;
        throw SQLExecError(index_name + " not found in _indices");
    }

    // Drop the index
    DbIndex &index = SQLExec::indices->get_index(index_name, table_name);
    index.drop();

    // Remove the index from the _indices table
    for (Handle const &handle : *handles)
        SQLExec::indices->del(handle);
    delete handles;

    return new QueryResult("dropped index " + index_name);
}
