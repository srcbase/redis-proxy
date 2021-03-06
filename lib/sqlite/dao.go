package sqlite

import (
	. "github.com/luoxiaojun1992/redis-proxy/lib/helper"
)

func GetClientNum() uint64 {
	stmt, err := SqliteConn.Prepare("select value from stats where metric = 'client_num'")
	CheckErr(err)
	defer stmt.Close()

	var client_num uint64
	query_err := stmt.QueryRow().Scan(&client_num)
	CheckErr(query_err)

	return client_num
}

func UpdateClientNum(client_num *uint64) {
	stmt, err := SqliteConn.Prepare("UPDATE stats SET value = ? WHERE metric = 'client_num'")
	CheckErr(err)
	defer stmt.Close()

	_, exec_err := stmt.Exec(client_num)
	CheckErr(exec_err)
}
