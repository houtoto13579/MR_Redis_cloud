#!/bin/bash

#192.168.107.101
VM_LIST=`cat ./reverse_hosts | grep iiscloud | awk -F ' ' '{print $1}'`
#NS_PATH="/usr/local/hadoop/conf"
#NS_LIST=`ls ${CONF_PATH} | grep site | sed s/~//g | uniq`

#rm -rf  /hadoop_disk/hdfs_tmp/dfs/data

for v in $VM_LIST; do
    echo "reboot "$v
    #ssh -t $v "sudo /etc/init.d/memcached stop; sudo /etc/init.d/memcached start"
    ssh -t $v "sudo reboot"
    #ssh -t $v "sudo /etc/init.d/memcached stop"
    #ssh -t $v "sudo rm /var/log/memcached.log"
done

