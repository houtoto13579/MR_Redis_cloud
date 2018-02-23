#!/bin/bash

#192.168.107.101
VM_LIST=`cat ./hosts | grep iiscloud | awk -F ' ' '{print $1}'`
#NS_PATH="/usr/local/hadoop/conf"
#NS_LIST=`ls ${CONF_PATH} | grep site | sed s/~//g | uniq`

#rm -rf  /hadoop_disk/hdfs_tmp/dfs/data


for v in $VM_LIST; do
    echo "delete redis cli in "$v
    #redis-cli -h $v CLIENT LIST | sed -n 's|.*addr=\(.*\)\:.*|\1|p' | sort | uniq -c
    redis-cli -h $v -p 6379 ping
    redis-cli -h $v CLIENT KILL TYPE normal
done

