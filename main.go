package main

import (
	"database/sql"
	"fmt"
	"github.com/howeyc/fsnotify"
	. "github.com/luoxiaojun1992/redis-proxy/lib/helper"
	. "github.com/luoxiaojun1992/redis-proxy/lib/monitor"
	_ "github.com/mattn/go-sqlite3"
	"github.com/robfig/config"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type RedisConn struct {
	Lock *sync.Mutex
	Conn net.Conn
}

var redis_conns []*RedisConn

var start_index int
var start_index_lock sync.Mutex

var c *config.Config
var err_c error

var ip_white_list_arr []string

var client_num uint64

var ip_white_list_lock sync.Mutex

var sqlite_conn *sql.DB

func main() {
	c, err_c = config.ReadDefault("./config/sample.config.cfg")
	CheckErr(err_c)

	parseIpWhiteList()

	connectSqlite()
	defer sqlite_conn.Close()
	loadStatsData()
	go statsPersistent()

	Monitor_signal = make(chan bool)

	go watchFile("./config/sample.config.cfg")

	go Monitor(&client_num, c)

	connectRedis()

	for _, redis_conn := range redis_conns {
		defer redis_conn.Conn.Close()
	}

	startServer()
}

/**
 * Parse ip white list
 */
func parseIpWhiteList() {
	ip_white_list, err_ip_white_list := c.String("access-control", "ip-white-list")
	CheckErr(err_ip_white_list)
	if ip_white_list != "" {
		ip_white_list_lock.Lock()
		ip_white_list_arr = strings.Split(ip_white_list, ",")
		ip_white_list_lock.Unlock()
	}
}

/**
 * Set redis connection pool
 */
func connectRedis() {
	redis_host, err_redis_host := c.String("redis-server", "host")
	CheckErr(err_redis_host)

	redis_port, err_redis_port := c.String("redis-server", "port")
	CheckErr(err_redis_port)

	redis_password, err_redis_password := c.String("redis-server", "password")
	CheckErr(err_redis_password)

	for i := 0; i < REDIS_CONNS_TOTAL; i++ {
		redis_conn, err := net.Dial("tcp", redis_host+":"+redis_port)
		CheckErr(err)

		_, err2 := redis_conn.Write([]byte("AUTH " + redis_password + "\r\nSELECT 0\r\n"))
		CheckErr(err2)

		buf := make([]byte, 4096)
		redis_conn.Read(buf)

		redisConn := new(RedisConn)
		redisConnLock := new(sync.Mutex)
		redisConn.Conn = redis_conn
		redisConn.Lock = redisConnLock

		redis_conns = append(redis_conns, redisConn)
	}
}

/**
 * Start tcp server
 */
func startServer() {
	fmt.Println("Starting redis proxy...")

	tcp_server_port, err_tcp_server_port := c.String("tcp-server", "port")
	CheckErr(err_tcp_server_port)

	l, err := net.Listen("tcp", "0.0.0.0:"+tcp_server_port)
	CheckErr(err)

	for {
		conn, err2 := l.Accept()
		CheckErr(err2)

		if !checkIp(conn) {
			continue
		}

		if client_num < MAX_CLIENT_NUM {
			client_num++
		} else {
			client_num = 1
		}

		go handler(conn)
	}
}

/**
 * Check ip limit
 */
func checkIp(conn net.Conn) bool {
	ip_white_list_lock.Lock()
	if len(ip_white_list_arr) > 0 {
		host, _, err_host_port := net.SplitHostPort(conn.RemoteAddr().String())
		if err_host_port != nil || !InStringArray(host, ip_white_list_arr) {
			conn.Close()
			ip_white_list_lock.Unlock()
			return false
		}
	}
	ip_white_list_lock.Unlock()

	return true
}

/**
 * Handle tcp request
 */
func handler(conn net.Conn) {
	fmt.Println("Accepted Connection from ", conn.RemoteAddr())

	defer conn.Close()

	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf[0:])

		command := string(buf[0:n])

		if err != nil || strings.Contains(command, "COMMAND") {
			break
		}

		if n > 0 && commandFilter(command) {
			go exec(buf[0:n], conn)
		} else {
			conn.Write([]byte("+OK\r\n"))
		}
	}
}

/**
 * Filter dangerous commands
 */
func commandFilter(command string) bool {
	banned_commands := []string{"flushall", "flushdb", "keys", "auth"}
	additional_banned_commands, additional_banned_commands_err := c.String("security-review", "banned-commands")
	CheckErr(additional_banned_commands_err)
	if additional_banned_commands != "" {
		additional_banned_commands_arr := strings.Split(additional_banned_commands, ",")
		for _, additional_banned_command := range additional_banned_commands_arr {
			banned_commands = append(banned_commands, additional_banned_command)
		}
	}

	command = strings.ToLower(command)
	for _, banned_command := range banned_commands {
		if strings.Contains(command, banned_command) {
			return false
		}
	}

	return true
}

/**
 * Get one redis connection
 */
func getRedisConn() *RedisConn {
	start_index_lock.Lock()
	if start_index >= REDIS_CONNS_TOTAL-1 {
		start_index = 0
	} else {
		start_index++
	}
	start_index_lock.Unlock()

	fmt.Println("Using redis connection ", start_index)
	return redis_conns[start_index]
}

/**
 * Exec redis command
 */
func exec(command []byte, conn net.Conn) {
	redis_conn := getRedisConn()

	redis_conn.Lock.Lock()

	_, err := redis_conn.Conn.Write(command)
	CheckErr(err)

	buf := make([]byte, 65535)
	resp := ""
	for {
		n, err2 := redis_conn.Conn.Read(buf[0:])
		CheckErr(err2)
		resp += string(buf[0:n])
		if n <= 65535 {
			break
		}
	}

	conn.Write([]byte(resp))

	redis_conn.Lock.Unlock()
}

/**
 * Watch File
 */
func watchFile(filename string) {
	watcher, _ := fsnotify.NewWatcher()

	go func() {
		for {
			select {
			case ev := <-watcher.Event:
				if ev.IsModify() {
					fmt.Println("Config file modified.")

					// Restart telegraf monitor
					Monitor_signal <- true
					go Monitor(&client_num, c)

					// Reset ip white list
					parseIpWhiteList()

					watcher.Watch(filename)
				}
			case err := <-watcher.Error:
				fmt.Println(err)
			}
		}
	}()

	watcher.Watch(filename)
}

/**
 * Connect sqlite3
 */
func connectSqlite() {
	var sqlite_conn_err error
	sqlite_conn, sqlite_conn_err = sql.Open("sqlite3", "./redis_proxy.db")
	CheckErr(sqlite_conn_err)

	createTableSqlStmt := `create table if not exists stats (id integer not null primary key, metric string not null default "", value integer not null default 0)`
	_, create_table_err := sqlite_conn.Exec(createTableSqlStmt)
	CheckErr(create_table_err)
}

/**
 * Load stats data from db
 */
func loadStatsData() {
	stmt, err := sqlite_conn.Prepare("select value from stats where metric = 'client_num'")
	CheckErr(err)
	defer stmt.Close()

	query_err := stmt.QueryRow().Scan(&client_num)
	CheckErr(query_err)
}

/**
 * Stats data persistent
 */
func statsPersistent() {
	stmt, err := sqlite_conn.Prepare("UPDATE stats SET value = ? WHERE metric = 'client_num'")
	CheckErr(err)
	defer stmt.Close()

	frequency, frequency_err := c.String("stats-persistent", "frequency")
	CheckErr(frequency_err)
	if frequency == "" {
		frequency = "1"
	}
	frequency_num, err_frequency_num := strconv.Atoi(frequency)
	CheckErr(err_frequency_num)

	for {
		_, exec_err := stmt.Exec(client_num)
		CheckErr(exec_err)

		t := time.NewTimer(time.Second * time.Duration(frequency_num))
		<-t.C
	}
}
