package monitor

import (
	"fmt"
	. "github.com/luoxiaojun1992/redis-proxy/lib/helper"
	"github.com/robfig/config"
	"net"
	"time"
)

/**
 * Get telegraf tcp connection
 */
func GetTelegrafConn(c *config.Config) net.Conn {
	telegraf_monitor_host, telegraf_monitor_host_err := c.String("telegraf-monitor", "host")
	CheckErr(telegraf_monitor_host_err)
	telegraf_monitor_port, telegraf_monitor_port_err := c.String("telegraf-monitor", "port")
	CheckErr(telegraf_monitor_port_err)
	telegraf_conn, err := net.Dial("tcp", telegraf_monitor_host+":"+telegraf_monitor_port)
	CheckErr(err)

	return telegraf_conn
}

/**
 * Telegraf monitor
 */
func Monitor(client_num *uint64, c *config.Config) {
	monitor_lock.Lock()
	defer monitor_lock.Unlock()

	telegraf_conn := GetTelegrafConn(c)
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
			telegraf_conn = GetTelegrafConn(c)
		}

		t := time.NewTimer(time.Second * time.Duration(1))
		<-t.C
	}
}
