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
    if(column_names!= nullptr)
        delete column_names;
    if(column_attributes!=nullptr)
        delete column_attributes;
    if(rows!= nullptr){
        for(auto row:*rows){
              delete row;
        }
        delete rows;
    }

}


QueryResult *SQLExec::execute(const SQLStatement *statement) {

    //initializes _tables table, if not yet present
    if(SQLExec::tables == nullptr)
        SQLExec::tables=new Tables();

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

    column_name=col->name;
    switch(col->type){
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

// CREATE
QueryResult *SQLExec::create(const CreateStatement *statement) {


    Identifier table_name=statement->tableName;
    ColumnNames column_names;
    ColumnAttributes column_attributes;

    Identifier column_name;
    ColumnAttribute columnAttribute;

    //add column_names & identify columnAttributes
    for(auto *col: *statement->columns){
        column_definition(col,column_name,columnAttribute);
        column_names.push_back(column_name);
        column_attributes.push_back(columnAttribute);

    }

    //update _tables schema
    ValueDict *row;
    row->at("table_name")= table_name;
    Handle t_handle=SQLExec::tables->insert(row);


    try {
        //update _columns schema
        Handles c_handles;
        DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (uint i = 0; i < column_names.size(); i++) {
                row->at("column_name") = column_names[i];
                row->at("data_type") = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                c_handles.push_back(columns.insert(row));  // Insert into _columns
            }

            //create the relation
            DbRelation &table =SQLExec::tables->get_table(table_name);
            if(statement->ifNotExists){
                table.create_if_not_exists();
            }else{
                table.create();
            }

            }
            catch(DbRelationError &e){
             //attempt to undo the insertions into _columns
                try{
                    for(Handle handle:c_handles){
                        columns.del(handle);
                    }

                   }catch(exception &e) {
                throw SQLExecError(string("Error: ") + e.what());
            }

        }


       }catch(exception &e){
        //attempt to undo the insertion into _tables
        try{
           SQLExec::tables->del(t_handle);
        }
        catch(exception &e) {
            throw SQLExecError(string("Error: ") + e.what());
        }

    }


    return new QueryResult(std::string("Created")+ table_name);
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    return new QueryResult("not implemented"); // FIXME
}


QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch(statement->type){
        case ShowStatement::kTables:
            return show_tables();
            break;
        case ShowStatement::kColumns:
            return show_columns(statement);
            break;
        default:
            throw SQLExecError("Unrecognized show type!");

    }

}

QueryResult *SQLExec::show_tables() {
    std::cout<<"SHOW TABLES"<<std::endl;
    std::cout<<"table_name"<<std::endl;
    std::cout<<"+---------+"<<std::endl;


    return new QueryResult(""); // FIXME
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    return new QueryResult("not implemented"); // FIXME
}

