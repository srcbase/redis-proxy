<?php
$redis = new \Redis();
$redis->connect('127.0.0.1', '63799', 0.01);
var_dump($redis->info());
var_dump($redis->info());
