package sqlite

import (
	"database/sql"
	. "github.com/luoxiaojun1992/redis-proxy/lib/helper"
)

var SqliteConn *sql.DB

/**
 * Connect sqlite3
 */
func ConnectSqlite() *sql.DB {
	var sqlite_conn_err error
	SqliteConn, sqlite_conn_err = sql.Open("sqlite3", "./redis_proxy.db")
	CheckErr(sqlite_conn_err)

	createTableSqlStmt := `create table if not exists stats (id integer not null primary key, metric string not null default "", value integer not null default 0)`
	_, create_table_err := SqliteConn.Exec(createTableSqlStmt)
	CheckErr(create_table_err)

	return SqliteConn
}
