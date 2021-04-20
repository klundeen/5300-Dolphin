#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cassert>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"
#include "heap_storage.h"

using namespace std;
using namespace hsql;

string operatorExpressionToString(const Expr *expr);

/**
 * Convert the hyrise Expr AST back into the equivalent SQL
 * @param expr expression to unparse
 * @return     SQL equivalent to *expr
 */
string expressionToString(const Expr *expr) {
    string rStr;
    switch (expr->type) {
        case kExprStar:
            rStr += "*";
            break;
        case kExprColumnRef:
            if (expr->table != NULL)
                rStr += string(expr->table) + ".";
        case kExprLiteralString:
            rStr += expr->name;
            break;
        case kExprLiteralFloat:
            rStr+= to_string(expr->fval);
            break;
        case kExprLiteralInt:
            rStr += to_string(expr->ival);
            break;
        case kExprFunctionRef:
            rStr += string(expr->name) + "?" + expr->expr->name;
            break;
        case kExprOperator:
            rStr += operatorExpressionToString(expr);
            break;
        default:
            rStr += "???";  // in case there are exprssion types we don't know about here
            break;
    }
    if (expr->alias != NULL)
        rStr += string(" AS ") + expr->alias;
    return rStr;
}

/**
 * Convert the hyrise Expr AST for an operator expression back into the equivalent SQL
 * @param expr operator expression to unparse
 * @return     SQL equivalent to *expr
 */
string operatorExpressionToString(const Expr *expr) {
    if (expr == NULL)
        return "null";

    string rStr;
    // Unary prefix operator: NOT
    if (expr->opType == Expr::NOT)
        rStr += "NOT ";

    // Left-hand side of expression
    rStr += expressionToString(expr->expr) + " ";

    // Operator itself
    switch (expr->opType) {
        case Expr::SIMPLE_OP:
            rStr += expr->opChar;
            break;
        case Expr::AND:
            rStr += "AND";
            break;
        case Expr::OR:
            rStr += "OR";
            break;
        default:
            break; // e.g., for NOT
    }

    // Right-hand side of expression (only present for binary operators)
    if (expr->expr2 != NULL)
        rStr += " " + expressionToString(expr->expr2);
    return rStr;
}

/**
 * Convert the hyrise TableRef AST back into the equivalent SQL
 * @param table  table reference AST to unparse
 * @return       SQL equivalent to *table
 */
string tableRefInfoToString(const TableRef *table) {
    string rStr;
    switch (table->type) {
        case kTableSelect:
            rStr += "kTableSelect FIXME";
            break;
        case kTableName:
            rStr += table->name;
            if (table->alias != NULL)
                rStr += string(" AS ") + table->alias;
            break;
        case kTableJoin:
            rStr += tableRefInfoToString(table->join->left);
            switch (table->join->type) {
                case kJoinCross:
                case kJoinInner:
                    rStr += " JOIN ";
                    break;
                case kJoinOuter:
                case kJoinLeftOuter:
                case kJoinLeft:
                    rStr += " LEFT JOIN ";
                    break;
                case kJoinRightOuter:
                case kJoinRight:
                    rStr += " RIGHT JOIN ";
                    break;
                case kJoinNatural:
                    rStr += " NATURAL JOIN ";
                    break;
            }
            rStr += tableRefInfoToString(table->join->right);
            if (table->join->condition != NULL)
                rStr += " ON " + expressionToString(table->join->condition);
            break;
        case kTableCrossProduct:
            bool doComma = false;
            for (TableRef *tbl : *table->list) {
                if (doComma)
                    rStr += ", ";
                rStr += tableRefInfoToString(tbl);
                doComma = true;
            }
            break;
    }
    return rStr;
}

/**
 * Convert the hyrise ColumnDefinition AST back into the equivalent SQL
 * @param col  column definition to unparse
 * @return     SQL equivalent to *col
 */
string columnDefinitionToString(const ColumnDefinition *col) {
    string rStr(col->name);
    switch (col->type) {
        case ColumnDefinition::DOUBLE:
            rStr += " DOUBLE";
            break;
        case ColumnDefinition::INT:
            rStr += " INT";
            break;
        case ColumnDefinition::TEXT:
            rStr += " TEXT";
            break;
        default:
            rStr += " ...";
            break;
    }
    return rStr;
}

/**
 * Execute an SQL select statement (but for now, just spit back the SQL)
 * @param stmt  Hyrise AST for the select statement
 * @returns     a string (for now) of the SQL statment
 */
string executeSelect(const SelectStatement *stmt) {
    string rStr("SELECT ");
    bool doComma = false;
    for (Expr *expr : *stmt->selectList) {
        if (doComma)
            rStr += ", ";
        rStr += expressionToString(expr);
        doComma = true;
    }
    rStr += " FROM " + tableRefInfoToString(stmt->fromTable);
    if (stmt->whereClause != NULL)
        rStr += " WHERE " + expressionToString(stmt->whereClause);
    return rStr;
}

/**
 * Execute an SQL insert statement (but for now, just spit back the SQL)
 * @param stmt  Hyrise AST for the insert statement
 * @returns     a string (for now) of the SQL statment
 */
string executeInsert(const InsertStatement *stmt) {
    return "INSERT ...";
}

/**
 * Execute an SQL create statement (but for now, just spit back the SQL)
 * @param stmt  Hyrise AST for the create statement
 * @returns     a string (for now) of the SQL statment
 */
string executeCreate(const CreateStatement *stmt) {
    string rStr("CREATE TABLE ");
    if (stmt->type != CreateStatement::kTable)
         return rStr + "...";
    if (stmt->ifNotExists)
        rStr += "IF NOT EXISTS ";
    rStr += string(stmt->tableName) + " (";
    bool doComma = false;
    for (ColumnDefinition *col : *stmt->columns) {
        if (doComma)
            rStr += ", ";
        rStr += columnDefinitionToString(col);
        doComma = true;
    }
    rStr += ")";
    return rStr;
}

/**
 * Execute an SQL statement (but for now, just spit back the SQL)
 * @param stmt  Hyrise AST for the statement
 * @returns     a string (for now) of the SQL statment
 */
string execute(const SQLStatement *stmt) {
    switch (stmt->type()) {
        case kStmtSelect:
            return executeSelect((const SelectStatement *) stmt);
        case kStmtInsert:
            return executeInsert((const InsertStatement *) stmt);
        case kStmtCreate:
            return executeCreate((const CreateStatement *) stmt);
        default:
            return "Not yet implemented";
    }
}

/**
 * Main entry point of the sql5300 program
 * @args dbenvpath  the path to the BerkeleyDB database environment
 */
int main(int argc, char **argv) {

    // Open or create the db enviroment
    if (argc != 2) {
        cerr << "Usage: cpsc5300: dbenvpiath" << endl;
        return 1;
    }
    char *envHome = argv[1];
    cout << "(sql5300: running with database at " << envHome << ")" << endl;
    DbEnv env(0U);
    env.set_message_stream(&cout);
    env.set_error_stream(&cerr);
    try {
        env.open(envHome, DB_CREATE | DB_INIT_MPOOL, 0);
    } catch (DbException &exc) {
        cerr << "(sql5300: " << exc.what() << ")";
        exit(1);
    }

    while (true) {
        cout << "SQL> ";
        string query;
        getline(cin, query);
        if (query.length() == 0)
            continue;  //skip
        if (query == "quit")
            break;  //quit out
        if (query == "test")
            cout << "testing: " << test_heap_storage() << endl;
        
        SQLParserResult *result = SQLParser::parseSQLString(query);
        if (!result->isValid()) {
            cout << "invalid SQL: " << query << endl;
            continue;
        }

        //run execute fn
        for (uint i = 0; i < result->size(); ++i) {
            cout << execute(result->getStatement(i)) << endl;
        }
    }
    return EXIT_SUCCESS;
}
