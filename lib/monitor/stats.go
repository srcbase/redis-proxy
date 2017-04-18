package monitor

import (
	"database/sql"
	. "github.com/luoxiaojun1992/redis-proxy/lib/helper"
	. "github.com/luoxiaojun1992/redis-proxy/lib/sqlite"
	"github.com/robfig/config"
	"strconv"
	"time"
)

/**
 * Load stats data from db
 */
func LoadStatsData() uint64 {
	return GetClientNum()
}

/**
 * Stats data persistent
 */
func StatsPersistent(sqlite_conn *sql.DB, client_num *uint64, c *config.Config) {
	frequency, frequency_err := c.String("stats-persistent", "frequency")
	CheckErr(frequency_err)
	if frequency == "" {
		frequency = "1"
	}
	frequency_num, err_frequency_num := strconv.Atoi(frequency)
	CheckErr(err_frequency_num)

	for {
		UpdateClientNum(client_num)

		t := time.NewTimer(time.Second * time.Duration(frequency_num))
		<-t.C
	}
}
