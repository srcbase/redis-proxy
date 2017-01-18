# redis-proxy
[![Build Status](https://travis-ci.org/luoxiaojun1992/redis-proxy.svg?branch=master)](https://travis-ci.org/luoxiaojun1992/redis-proxy)

## Features
+ Redis Connection Pool (Up to 200)
+ Integration with telegraf
+ Command Filter
+ Client IP Limit
+ Data Sharding(No transaction support)
+ Transaction Support(Only support for first node)

## Installation
+ Execute install.sh in root/script directory

## Configuration
```
[redis-server]
host: 127.0.0.1(Separated by commas)
port: 6379
password: (Optional)

[tcp-server]
port: 63799

[access-control]
ip-white-list: (Optional, separated by commas)

; Telegraf Monitor Tcp Listener
[telegraf-monitor]
host: 127.0.0.1 (Optional)
port: 8094 (Optional)

[security-review]
banned-commands: (Optional, separated by commas. Default: flushall,flushdb,keys,auth)

[stats-persistent]
frequency: 1
```

## Usage
```
redis-cli -p 63799
```

