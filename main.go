package main

import (
	"database/sql"
	"fmt"
	"github.com/howeyc/fsnotify"
	. "github.com/luoxiaojun1992/redis-proxy/lib/helper"
	_ "github.com/mattn/go-sqlite3"
	"github.com/robfig/config"
	"net"
	"strings"
	"sync"
	"time"
)

type RedisConn struct {
	Lock *sync.Mutex
	Conn net.Conn
}

var redis_conns []*RedisConn

const REDIS_CONNS_TOTAL = 200

var start_index int
var start_index_lock sync.Mutex

var c *config.Config
var err_c error

var ip_white_list_arr []string

var client_num uint64

const MAX_CLIENT_NUM = 18446744073709551615

var monitor_signal chan bool
var monitor_lock sync.Mutex

var ip_white_list_lock sync.Mutex

func main() {
	c, err_c = config.ReadDefault("./config/sample.config.cfg")
	if err_c != nil {
		panic(err_c)
	}

	parseIpWhiteList()

	connectSqlite()

	monitor_signal = make(chan bool)

	go watchFile("./config/sample.config.cfg")

	go monitor()

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
	if err_ip_white_list != nil {
		panic(err_ip_white_list)
	}
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
	if err_redis_host != nil {
		panic(err_redis_host)
	}

	redis_port, err_redis_port := c.String("redis-server", "port")
	if err_redis_port != nil {
		panic(err_redis_port)
	}

	redis_password, err_redis_password := c.String("redis-server", "password")
	if err_redis_password != nil {
		panic(err_redis_password)
	}

	for i := 0; i < REDIS_CONNS_TOTAL; i++ {
		redis_conn, err := net.Dial("tcp", redis_host+":"+redis_port)
		if err != nil {
			panic(err)
		}

		_, err2 := redis_conn.Write([]byte("AUTH " + redis_password + "\r\nSELECT 0\r\n"))
		if err2 != nil {
			panic(err2)
		}

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
	if err_tcp_server_port != nil {
		panic(err_tcp_server_port)
	}

	l, err := net.Listen("tcp", "0.0.0.0:"+tcp_server_port)
	if err != nil {
		panic(err)
	}

	for {
		conn, err2 := l.Accept()
		if err2 != nil {
			panic(err2)
		}

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
	if additional_banned_commands_err != nil {
		panic(additional_banned_commands_err)
	}
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
	if err != nil {
		panic(err)
	}

	buf := make([]byte, 65535)
	resp := ""
	for {
		n, err2 := redis_conn.Conn.Read(buf[0:])
		if err2 != nil {
			panic(err2)
		}
		resp += string(buf[0:n])
		if n <= 65535 {
			break
		}
	}

	conn.Write([]byte(resp))

	redis_conn.Lock.Unlock()
}

/**
 * Get telegraf tcp connection
 */
func getTelegrafConn() net.Conn {
	telegraf_monitor_host, telegraf_monitor_host_err := c.String("telegraf-monitor", "host")
	if telegraf_monitor_host_err != nil {
		panic(telegraf_monitor_host_err)
	}
	telegraf_monitor_port, telegraf_monitor_port_err := c.String("telegraf-monitor", "port")
	if telegraf_monitor_port_err != nil {
		panic(telegraf_monitor_port_err)
	}
	telegraf_conn, err := net.Dial("tcp", telegraf_monitor_host+":"+telegraf_monitor_port)
	if err != nil {
		panic(err)
	}

	return telegraf_conn
}

/**
 * Telegraf monitor
 */
func monitor() {
	monitor_lock.Lock()
	defer monitor_lock.Unlock()

	telegraf_conn := getTelegrafConn()
	defer telegraf_conn.Close()

	fmt.Println("Monitor started.")

	for {
		select {
		case ev := <-monitor_signal:
			if ev {
				fmt.Println("Monitor exited.")
				return
			}
		default:
			//
		}

		_, err := telegraf_conn.Write([]byte("redis_proxy client_count=" + fmt.Sprintf("%d", client_num) + "\n"))
		if err != nil {
			telegraf_conn = getTelegrafConn()
		}

		t := time.NewTimer(time.Second * time.Duration(1))
		<-t.C
	}
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
					monitor_signal <- true
					go monitor()

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

func connectSqlite() {
	db, err := sql.Open("sqlite3", "./redis_proxy.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	sqlStmt := `create table if not exists stats (id integer not null primary key, client_num integer not null default 0);`
	_, err2 := db.Exec(sqlStmt)
	if err2 != nil {
		panic(err2)
	}

	stmt, err3 := db.Prepare("INSERT INTO stats(client_num) values(?)")
	if err3 != nil {
		panic(err3)
	}

	_, err4 := stmt.Exec(0)
	if err4 != nil {
		panic(err4)
	}
}
