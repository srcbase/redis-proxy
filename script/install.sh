#! /bin/bash

echo 'Start installing...'
go get -v github.com/howeyc/fsnotify
go get -v github.com/robfig/config
go get -v github.com/mattn/go-sqlite3
go get -v github.com/cznic/sortutil
go install ..
echo 'Installation completed'

