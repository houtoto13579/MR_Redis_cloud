#!/bin/bash

#192.168.107.101
VM_LIST=`cat ./hosts | grep iiscloud | awk -F ' ' '{print $1}'`
#NS_PATH="/usr/local/hadoop/conf"
#NS_LIST=`ls ${CONF_PATH} | grep site | sed s/~//g | uniq`

#rm -rf  /hadoop_disk/hdfs_tmp/dfs/data

for v in $VM_LIST; do
    echo "create the 10GB ramdisk in "$v
    #ssh -t $v "sudo mkdir -p /media/ramdisk"
    #ssh -t $v "sudo mount -t tmpfs -o size=10G tmpfs /media/ramdisk"
    #ssh -t $v "sudo umount -t tmpfs /media/ramdisk"
    #ssh -t $v "sudo chown -R hduser:hadoop /media/ramdisk"
    #ssh -t $v "sudo apt-get upgrade"
    #ssh -t $v "sudo /etc/init.d/memcached restart"
    #ssh -t $v "sudo /etc/init.d/redis-server restart"
    ssh -t $v "rm ~/part*"
done

