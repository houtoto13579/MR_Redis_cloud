#!/bin/bash

#192.168.107.101
VM_LIST=`cat ./hosts | grep iiscloud | awk -F ' ' '{print $1}'`
#NS_PATH="/usr/local/hadoop/conf"
#NS_LIST=`ls ${CONF_PATH} | grep site | sed s/~//g | uniq`

#rm -rf  /hadoop_disk/hdfs_tmp/dfs/data


for v in $VM_LIST; do
    echo "==================="
    echo "check redis in "$v
    #ssh -t $v "sudo mkdir -p /usr/local/zookeeper; sudo chown -R hduser:hadoop /usr/local/zookeeper"
    #ssh -t $v "sudo mkdir -p /media/ramdisk"
    #ssh -t $v "sudo mount -t tmpfs -o size=10G tmpfs /media/ramdisk"
    #ssh -t $v "sudo /etc/init.d/redis-server start"
    #ssh -t $v "sudo scp hduser@iiscloud01:/usr/bin/redis-server /usr/bin/"
    #scp /usr/bin/redis-server $v:~
    #ssh -t $v "pkill redis; cd ~; sudo ./redis-server /etc/redis/redis.conf"
    redis-cli -h $v -p 6379 ping
    redis-cli -h $v info memory
done
#for v in $VM_LIST; do
#    echo "check redis in "$v
#    #ssh -t $v "sudo mkdir -p /media/ramdisk"
#    #ssh -t $v "sudo mount -t tmpfs -o size=10G tmpfs /media/ramdisk"
#    #ssh -t $v "sudo /etc/init.d/redis-server restart"
#    #redis-cli -h $v -p 6379 ping
#    redis-cli -h $v FLUSHALL
#    echo "   "
#done

