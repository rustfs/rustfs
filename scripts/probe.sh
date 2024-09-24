#!/bin/bash

IP=127.0.0.1
PORT=9000

while true; do
    nc -zv ${IP} ${PORT}
    if [[ "$?" == "0" ]]; then
        exit 0
    fi
    sleep 2
done
