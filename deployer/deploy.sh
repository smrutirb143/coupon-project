#!/bin/sh
set -x 
cd `dirname $0`
cd ..
 
export GOOS=linux
rm -f main main.zip
go build main.go
zip main.zip main
