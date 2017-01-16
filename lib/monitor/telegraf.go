package monitor

import (
	"fmt"
	. "github.com/luoxiaojun1992/redis-proxy/lib/helper"
	"github.com/robfig/config"
	"net"
	"sync"
	"time"
)

var Monitor_signal chan bool
var Monitor_lock sync.Mutex

/**
 * Get telegraf tcp connection
 */
func getTelegrafConn(c *config.Config) (net.Conn, error) {
	telegraf_monitor_host, telegraf_monitor_host_err := c.String("telegraf-monitor", "host")
	CheckErr(telegraf_monitor_host_err)
	telegraf_monitor_port, telegraf_monitor_port_err := c.String("telegraf-monitor", "port")
	CheckErr(telegraf_monitor_port_err)
	telegraf_conn, err := net.Dial("tcp", telegraf_monitor_host+":"+telegraf_monitor_port)

	return telegraf_conn, err
}

/**
 * Telegraf monitor
 */
func Monitor(client_num *uint64, c *config.Config) {
	Monitor_lock.Lock()
	defer Monitor_lock.Unlock()

	telegraf_conn, err_telegraf_conn := getTelegrafConn(c)
	if err_telegraf_conn != nil {
		return
	}
	defer telegraf_conn.Close()

	fmt.Println("Monitor started.")

	for {
		select {
		case ev := <-Monitor_signal:
			if ev {
				fmt.Println("Monitor exited.")
				return
			}
		default:
			//
		}

		_, err := telegraf_conn.Write([]byte("redis_proxy client_count=" + fmt.Sprintf("%d", client_num) + "\n"))
		if err != nil {
			telegraf_conn, err_telegraf_conn = getTelegrafConn(c)
			if err_telegraf_conn != nil {
				return
			}
		}

		t := time.NewTimer(time.Second * time.Duration(1))
		<-t.C
	}
}
