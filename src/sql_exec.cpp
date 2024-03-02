/**
 * @file sql_exec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter Quarter 2024"
 */
#include "sql_exec.h"

using namespace std;
using namespace hsql;

// define constants
const Identifier TABLE_NAME = "table_name";
const Identifier INDEX_NAME = "index_name";
const Identifier COLUMN_NAME = "column_name";
const Identifier SEQ_IN_INDEX = "seq_in_index";
const Identifier INDEX_TYPE = "index_type";
const Identifier IS_UNIQUE = "is_unique";
const Identifier DATA_TYPE = "data_type";

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
        SQLExec::indices = new Indices();
    }

    try {
        switch (statement->type()) {
        case kStmtCreate:
            return create((const CreateStatement *)statement);
        case kStmtDrop:
            return drop((const DropStatement *)statement);
        case kStmtShow:
            return show((const ShowStatement *)statement);
        case kStmtInsert:
            return insert((const InsertStatement *)statement);
        case kStmtDelete:
            return del((const DeleteStatement *)statement);
        case kStmtSelect:
            return select((const SelectStatement *)statement);
        default:
            return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

QueryResult *SQLExec::insert(const InsertStatement *statement) {
    if (statement->type == InsertStatement::kInsertSelect)
        return new QueryResult("INSERT Select statement not yet implemented");

    Identifier table_name = statement->tableName;
    DbRelation &table = tables->get_table(table_name);
    ColumnNames column_names;
    auto values = (*statement->values);
    if (statement->columns == nullptr) {
        // if no columns given, use default table column order
        auto table_column_names = table.get_column_names();

        if (values.size() > table_column_names.size())
            throw SQLExecError("Too many values for columns");

        // unspecified columns are considered null values
        for (auto i = 0; i < values.size(); i++)
            column_names.push_back(table_column_names[i]);
    } else {
        // parser will make sure the number of columns and values are the same
        for (auto const &col : *statement->columns)
            column_names.push_back(col);
    }

    ValueDict row;
    for (auto i = 0; i < column_names.size(); i++) {
        string column_name = column_names[i];
        auto value = (*statement->values)[i];
        switch (value->type) {
        case kExprLiteralInt:
            row[column_name] = Value(value->ival);
            break;
        case kExprLiteralString:
            row[column_name] = Value(value->name);
            break;
        default:
            throw SQLExecError("Not supported data type");
        }
    }

    auto handle = table.insert(&row);

    auto index_names = indices->get_index_names(table_name);
    try {
        for (Identifier &index_name : index_names) {
            DbIndex &index = indices->get_index(table_name, index_name);
            index.insert(handle);
        }
    } catch (exception &e) {
        for (Identifier &index_name : index_names) {
            DbIndex &index = indices->get_index(table_name, index_name);
            // index.del(handle); // TODO: implement del in BTreeIndex
        }
        table.del(handle);
        throw;
    }

    auto index_size = index_names.size();
    string message = "successfully inserted 1 row into " + table_name;
    if (index_size > 0)
        message += " and " + to_string(index_size) + " indices";

    return new QueryResult(message);
}

void parse_expr(Expr *expr, ValueDict &where) {
    switch (expr->opType) {
    case Expr::AND:
        parse_expr(expr->expr, where);
        parse_expr(expr->expr2, where);
        break;
    case Expr::SIMPLE_OP: {
        Expr *col_expr = expr->expr;
        Expr *val_expr = expr->expr2;
        string column_name = col_expr->name;
        if (val_expr->type == kExprLiteralInt) {
            where[column_name] = Value(val_expr->ival);
        } else if (val_expr->type == kExprLiteralString) {
            where[column_name] = Value(val_expr->name);
        } else {
            throw SQLExecError("Not supported literal type");
        }
        break;
    }
    default:
        throw SQLExecError("Not supported operation type");
    }
}

QueryResult *SQLExec::del(const DeleteStatement *statement) {

    ValueDict where;
    if (statement->expr != nullptr)
        parse_expr(statement->expr, where);

    Identifier table_name = statement->tableName;
    DbRelation &table = tables->get_table(table_name);
    EvalPlan *relation = new EvalPlan(table);

    EvalPlan *plan = relation;
    if (!where.empty())
        plan = new EvalPlan(&where, relation);

    Handles *handles = plan->optimize()->pipeline().second;
    IndexNames index_names = SQLExec::indices->get_index_names(table_name);
    try {
        for (Identifier &index_name : index_names) {
            DbIndex &index = indices->get_index(table_name, index_name);
            for (Handle &handle : *handles) {
                // index.del(handle); // TODO: implement del in BTreeIndex
            }
        }

        for (Handle &handle : *handles)
            table.del(handle);
    } catch (exception &e) {
        // TODO: rollback
        throw;
    }
  
    string message = "successfully deleted " + to_string(handles->size()) +
                     " rows from " + table_name;
    if (index_names.size() > 0)
        message += " and " + to_string(index_names.size()) + " indices";

    return new QueryResult(message);
}

QueryResult *SQLExec::select(const SelectStatement *statement) {
    return new QueryResult("SELECT statement not yet implemented"); // FIXME
}

void SQLExec::column_definition(const ColumnDefinition *col,
                                Identifier &column_name,
                                ColumnAttribute &column_attribute) {
    column_name = col->name;
    switch (col->type) {
    case ColumnDefinition::INT:
        column_attribute.set_data_type(ColumnAttribute::INT);
        break;
    case ColumnDefinition::TEXT:
        column_attribute.set_data_type(ColumnAttribute::TEXT);
        break;
    case ColumnDefinition::DOUBLE:
    default:
        throw SQLExecError("unrecognized data type");
    }
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch (statement->type) {
    case CreateStatement::kTable:
        return create_table(statement);
    case CreateStatement::kIndex:
        return create_index(statement);
    default:
        return new QueryResult(
            "Only CREATE TABLE and CREATE INDEX are implemented");
    }
}

QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    Identifier table_name = statement->tableName;
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    Identifier column_name;
    ColumnAttribute column_attribute;
    for (ColumnDefinition *col : *statement->columns) {
        column_definition(col, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    // Add to schema: _tables and _columns
    ValueDict row;
    row[TABLE_NAME] = table_name;
    Handle t_handle = SQLExec::tables->insert(&row); // Insert into _tables
    try {
        Handles c_handles;
        DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (uint i = 0; i < column_names.size(); i++) {
                row[COLUMN_NAME] = column_names[i];
                row[DATA_TYPE] = Value(column_attributes[i].get_data_type() ==
                                               ColumnAttribute::INT
                                           ? "INT"
                                           : "TEXT");
                c_handles.push_back(
                    columns.insert(&row)); // Insert into _columns
            }

            // Finally, actually create the relation
            DbRelation &table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();

        } catch (...) {
            // attempt to remove from _columns
            try {
                for (auto const &handle : c_handles)
                    columns.del(handle);
            } catch (...) {
            }
            throw;
        }

    } catch (exception &e) {
        try {
            // attempt to remove from _tables
            SQLExec::tables->del(t_handle);
        } catch (...) {
        }
        throw;
    }
    return new QueryResult("created " + table_name);
}

QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    Identifier index_name = statement->indexName;
    Identifier table_name = statement->tableName;

    // get underlying relation
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // check that given columns exist in table
    const ColumnNames &table_columns = table.get_column_names();
    for (auto const &col_name : *statement->indexColumns)
        if (find(table_columns.begin(), table_columns.end(), col_name) ==
            table_columns.end())
            throw SQLExecError(string("Column '") + col_name +
                               "' does not exist in " + table_name);

    // insert a row for every column in index into _indices
    ValueDict row;
    row[TABLE_NAME] = Value(table_name);
    row[INDEX_NAME] = Value(index_name);
    row[INDEX_TYPE] = Value(statement->indexType);
    row[IS_UNIQUE] = Value(string(statement->indexType) ==
                           "BTREE"); // assume HASH is non-unique --
    int seq = 0;
    Handles i_handles;
    try {
        for (auto const &col_name : *statement->indexColumns) {
            row[SEQ_IN_INDEX] = Value(++seq);
            row[COLUMN_NAME] = Value(col_name);
            i_handles.push_back(SQLExec::indices->insert(&row));
        }

        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        index.create();

    } catch (...) {
        // attempt to remove from _indices
        try { // if any exception happens in the reversal below, we still want
              // to re-throw the original ex
            for (auto const &handle : i_handles)
                SQLExec::indices->del(handle);
        } catch (...) {
        }
        throw; // re-throw the original exception (which should give the client
               // some clue as to why it did
    }
    return new QueryResult("created index " + index_name);
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
    case DropStatement::kTable:
        return drop_table(statement);
    case DropStatement::kIndex:
        return drop_index(statement);
    default:
        return new QueryResult(
            "Only DROP TABLE and CREATE INDEX are implemented");
    }
}

QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    Identifier table_name = statement->name;
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
        throw SQLExecError("cannot drop a schema table");

    ValueDict where;
    where[TABLE_NAME] = Value(table_name);

    // get the table
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // remove any indices
    for (auto const &index_name :
         SQLExec::indices->get_index_names(table_name)) {
        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        index.drop(); // drop the index
    }
    Handles *handles = SQLExec::indices->select(&where);
    for (auto const &handle : *handles)
        SQLExec::indices->del(handle); // remove all rows from _indices for each
                                       // index on this table
    delete handles;

    // remove from _columns schema
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    handles = columns.select(&where);
    for (auto const &handle : *handles)
        columns.del(handle);
    delete handles;

    // remove table
    table.drop();

    // finally, remove from _tables schema
    handles = SQLExec::tables->select(&where);
    SQLExec::tables->del(*handles->begin()); // expect only one row from select
    delete handles;

    return new QueryResult(string("dropped ") + table_name);
}

QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    Identifier table_name = statement->name;
    Identifier index_name = statement->indexName;

    // drop index
    DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
    index.drop();

    // remove rows from _indices for this index
    ValueDict where;
    where[TABLE_NAME] = Value(table_name);
    where[INDEX_NAME] = Value(index_name);
    Handles *handles = SQLExec::indices->select(&where);
    for (auto const &handle : *handles)
        SQLExec::indices->del(handle);
    delete handles;

    return new QueryResult("dropped index " + index_name);
}

// SHOW ...
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
    case ShowStatement::kTables:
        return show_tables();
    case ShowStatement::kColumns:
        return show_columns(statement);
    case ShowStatement::kIndex:
        return show_index(statement);
    default:
        throw SQLExecError("unrecognized SHOW type");
    }
}

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    ColumnNames *column_names = new ColumnNames;
    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_names->push_back(TABLE_NAME);
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back(INDEX_NAME);
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back(COLUMN_NAME);
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back(SEQ_IN_INDEX);
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::INT));

    column_names->push_back(INDEX_TYPE);
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back(IS_UNIQUE);
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::BOOLEAN));

    ValueDict where;
    where[TABLE_NAME] = Value(string(statement->tableName));
    Handles *handles = SQLExec::indices->select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle : *handles) {
        ValueDict *row = SQLExec::indices->project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}

QueryResult *SQLExec::show_tables() {
    ColumnNames *column_names = new ColumnNames;
    column_names->push_back(TABLE_NAME);

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles *handles = SQLExec::tables->select();
    u_long n = handles->size() - 3;

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle : *handles) {
        ValueDict *row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at(TABLE_NAME).s;
        if (table_name != Tables::TABLE_NAME &&
            table_name != Columns::TABLE_NAME &&
            table_name != Indices::TABLE_NAME)
            rows->push_back(row);
        else
            delete row;
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    ColumnNames *column_names = new ColumnNames;
    column_names->push_back(TABLE_NAME);
    column_names->push_back(COLUMN_NAME);
    column_names->push_back(DATA_TYPE);

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
    where[TABLE_NAME] = Value(statement->tableName);
    Handles *handles = columns.select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle : *handles) {
        ValueDict *row = columns.project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}
