package main

import (
	"fmt"
	. "github.com/luoxiaojun1992/redis-proxy/lib/helper"
	"github.com/robfig/config"
	"net"
	"strings"
	"sync"
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

func main() {
	c, err_c = config.ReadDefault("./config/sample.config.cfg")
	if err_c != nil {
		panic(err_c)
	}

	ip_white_list, err_ip_white_list := c.String("access-control", "ip-white-list")
	if err_ip_white_list != nil {
		panic(err_ip_white_list)
	}
	ip_white_list_arr = strings.Split(ip_white_list, ",")

	connectRedis()

	for _, redis_conn := range redis_conns {
		defer redis_conn.Conn.Close()
	}

	startServer()
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

		if len(ip_white_list_arr) > 0 {
			host, _, err_host_port := net.SplitHostPort(conn.RemoteAddr().String())
			if err_host_port != nil || !InStringArray(host, ip_white_list_arr) {
				conn.Close()
				continue
			}
		}

		go handler(conn)
	}
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

		if n > 0 && !strings.Contains(command, "AUTH") {
			go exec(buf[0:n], conn)
		}
	}
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
	n, err2 := redis_conn.Conn.Read(buf[0:])
	if err2 != nil {
		panic(err2)
	}

	conn.Write(buf[0:n])

	redis_conn.Lock.Unlock()
}
