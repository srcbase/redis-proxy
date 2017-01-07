package main

import (
	"fmt"
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

func main() {
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
	for i := 0; i < REDIS_CONNS_TOTAL; i++ {
		redis_conn, err := net.Dial("tcp", "demo.aliyuncs.com:6379")
		if err != nil {
			panic(err)
		}

		_, err2 := redis_conn.Write([]byte("AUTH demo\r\nSELECT 0\r\n"))
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
	l, err := net.Listen("tcp", "0.0.0.0:63799")
	if err != nil {
		panic(err)
	}

	for {
		conn, err2 := l.Accept()
		if err2 != nil {
			panic(err2)
		}

		go handler(conn)
	}
}

/**
 * Handle tcp accept
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
