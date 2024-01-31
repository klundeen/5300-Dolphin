/**
 * program to execute SQL commands
 * 
 * By: Jou Ho
 * For: CPSC 5300, WQ2024
 */

#include <iostream>
#include <sstream>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cstdio>
#include "SQLParser.h"
#include "db_cxx.h"
#include "heap_storage.h"

DbEnv *_DB_ENV;

using namespace std;
using namespace hsql;

// Utility functions and classes
class SQLParserHelper {
public:
    static std::string unparse(const hsql::SQLStatement* stmt) {
        switch (stmt->type()) {
            case hsql::kStmtCreate:
                return unparseCreateTable(static_cast<const hsql::CreateStatement*>(stmt));
            case hsql::kStmtSelect:
                return unparseSelect(static_cast<const hsql::SelectStatement*>(stmt));
            default:
                return "Unsupported SQL statement";
        }
    }

private:
    static std::string unparseCreateTable(const hsql::CreateStatement* stmt) {
        std::string sql = "CREATE TABLE " + std::string(stmt->tableName);
        sql += " (";
        for (auto* col : *stmt->columns) {
            sql += columnDefinitionToString(col) + ", ";
        }
        sql.pop_back(); // Remove last comma
        sql.pop_back(); // Remove last space
        sql += ")";
        return sql;
    }

    static std::string columnDefinitionToString(const hsql::ColumnDefinition* col) {
        std::string ret(col->name);
        switch(col->type) {
            case hsql::ColumnDefinition::DOUBLE:
                ret += " DOUBLE";
                break;
            case hsql::ColumnDefinition::INT:
                ret += " INT";
                break;
            case hsql::ColumnDefinition::TEXT:
                ret += " TEXT";
                break;
            default:
                ret += " ...";
                break;
        }
        return ret;
    }

    static std::string unparseSelect(const hsql::SelectStatement* stmt) {
        std::string sql = "SELECT ";
        
        // Handle SELECT fields
        for (const hsql::Expr* expr : *stmt->selectList) {
            sql += (expr == stmt->selectList->front() ? "" : ", ") + exprToString(expr);
        }
        
        // Handle FROM clause
        if (stmt->fromTable) {
            sql += " FROM " + tableRefToString(stmt->fromTable);
        }
    
        // Handle WHERE clause
        if (stmt->whereClause) {
            sql += " WHERE " + exprToString(stmt->whereClause);
        }
    
        return sql;
    }
    
    static std::string exprToString(const hsql::Expr* expr) {
        if (!expr) return "";
    
        std::string exprStr;
        switch (expr->type) {
            case hsql::kExprStar:
                exprStr = "*";
                break;
            case hsql::kExprColumnRef:
                if (expr->table) {
                    exprStr += std::string(expr->table) + ".";
                }
                exprStr += std::string(expr->name);
                break;
            case hsql::kExprOperator:
                exprStr = operatorExprToString(expr);
                break;
            // Handle other expression types as necessary
            default:
                exprStr = "<expr>";
        }
        return exprStr;
    }

    static std::string operatorExprToString(const hsql::Expr* expr) {
        if (!expr) return "";
    
        std::string opStr;
        if (expr->opType == hsql::Expr::SIMPLE_OP) {
            opStr = " " + std::string(1, expr->opChar) + " ";
        } else if (expr->opType == hsql::Expr::AND) {
            opStr = " AND ";
        } else if (expr->opType == hsql::Expr::OR) {
            opStr = " OR ";
        }
        // Add more cases for different operators
    
        return exprToString(expr->expr) + opStr + exprToString(expr->expr2);
    }
    
    static std::string tableRefToString(const hsql::TableRef* table) {
        if (!table) return "";
        
        std::string sql;
        switch (table->type) {
            case hsql::kTableName:
                sql = table->name;
                if (table->alias) sql += " AS " + std::string(table->alias);
                break;
            case hsql::kTableJoin:
                sql = "(" + tableRefToString(table->join->left);
                sql += " " + joinTypeToString(table->join->type);
                sql += " " + tableRefToString(table->join->right) + ")";
                if (table->join->condition) {
                    sql += " ON " + exprToString(table->join->condition);
                }
                break;
            case hsql::kTableCrossProduct:
                for (const hsql::TableRef* tbl : *table->list) {
                    sql += (tbl == table->list->front() ? "" : ", ") + tableRefToString(tbl);
                }
                break;
            // Handle other types as necessary
        }
        return sql;
    }
    
    static std::string joinTypeToString(const hsql::JoinType type) {
        switch (type) {
            case hsql::kJoinLeft: return "LEFT JOIN";
            case hsql::kJoinRight: return "RIGHT JOIN";
            // Add more cases as necessary
            default: return "JOIN";
        }
    }
};

// DBEnvironment class
class DBEnvironment {
public:
    explicit DBEnvironment(const std::string& path, const std::string& dbName)
        : env(0U), envDir(path), DBName(dbName), db(&env, 0) {
        // The constructor initializes the environment directory, the DbEnv, and Db objects
    }

    void open() {
        // Set the message and error streams for DbEnv
        env.set_message_stream(&std::cout);
        env.set_error_stream(&std::cerr);

        // Open the environment with the specified flags
        try {
            env.open(envDir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);
            std::cout << "Database environment opened at " << envDir << std::endl;

            // Open the database within the environment
            db.set_message_stream(env.get_message_stream());
            db.set_error_stream(env.get_error_stream());
            db.open(NULL, DBName.c_str(), NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644);
            std::cout << "Database " << DBName << " opened successfully." << std::endl;
            _DB_ENV = &env;
        } catch (DbException& e) {
            std::cerr << "Error in database environment: " << e.what() << std::endl;
            throw; // Rethrow the exception for the caller to handle
        }
    }
private:
    DbEnv env;
    std::string envDir;
    std::string DBName;
    Db db;
};

// SQLShell class
class SQLShell {
public:
    explicit SQLShell(const std::string& dbPath, const std::string& dbName) 
        : dbEnv(dbPath, dbName) {
        dbEnv.open();
        initialized = true;
    }

    void run() {
        std::cout << QUIT << " to end" << std::endl;

        while (true) {
            std::string sql;
            std::cout << "SQL> ";
            std::getline(std::cin, sql);

            if (sql == QUIT) {
                return;
            }

            if (sql == "test") {
                std::cout << "test_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << std::endl;
                continue;
            }

            hsql::SQLParserResult* result = hsql::SQLParser::parseSQLString(sql);
            if (result->isValid()) {
                std::cout << "Parsed successfully!" << std::endl;
                for (uint i = 0; i < result->size(); ++i) {
                    std::string statement = SQLParserHelper::unparse(result->getStatement(i));
                    std::cout << statement << std::endl;
                }
            } else {
                std::cerr << "Invalid SQL query." << std::endl;
                std::cerr << result->errorMsg() << " (Line: " << result->errorLine() << ", Column: " << result->errorColumn() << ")" << std::endl;
            }

            delete result;
        }
    }

private:
    DBEnvironment dbEnv;
    const std::string QUIT = "quit";
    bool initialized = false;

    void execute(const std::string& sqlStatement) {
        // Execute SQL statement
    }
};

// Main function
int main(int argc, char* argv[]) {
    // Open/create the database environment
    if (argc != 2) {
        cerr << "Usage: " << argv[0] << " <path to DB directory>" << endl;
        return 1;
    }
    string envHome = argv[1];
    cout << "(sql5300: running with database environment at " << envHome << ")" << endl;

    try {
        // Initialize the DB environment using the constructor
        DbEnv env(0U);
        env.set_message_stream(&cout);
        env.set_error_stream(&cerr);

        // Open the environment with the specified flags
        env.open(envHome.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

        // Assign the global variable to the newly opened environment
        _DB_ENV = &env;

        // SQL shell loop
        while (true) {
            cout << "SQL> ";
            string query;
            getline(cin, query);
            if (query.length() == 0)
                continue;  // Skip blank lines
            if (query == "quit")
                break;  // Exit the loop
	    if (query == "test") {
                cout << "test_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << endl;
                continue;
            }

            SQLParserResult* result = SQLParser::parseSQLString(query);
            if (result->isValid()) {
                cout << "Parsed successfully!" << endl;
                for (uint i = 0; i < result->size(); ++i) {
                    const SQLStatement* stmt = result->getStatement(i);
                    // Utilize your existing SQL execution or parsing logic here
                    string statement = SQLParserHelper::unparse(stmt);
                    cout << statement << endl;
                }
            } else {
                cerr << "Invalid SQL query: " << query << endl;
                cerr << result->errorMsg() << " (Line: " << result->errorLine() << ", Column: " << result->errorColumn() << ")" << endl;
            }

            delete result;
        }

    } catch (DbException& e) {
        cerr << "Database environment could not be opened: " << e.what() << endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
