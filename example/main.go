package main

import (
	"fmt"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:63799")
	if err != nil {
		panic(err)
	}

	for {
		conn.Write([]byte("INFO\r\n"))

		buf := make([]byte, 4096)
		conn.Read(buf[0:])

		fmt.Println(string(buf))
	}

	conn.Close()
}
