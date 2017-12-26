#!/bin/bash

HOSTS=$(cat ~/sys_sh/hosts | grep "iiscloud" | awk '{print $2}')
#DFS_PATH='/media/ramdisk/*'
DFS_PATH='/hadoop_disk/*'
LOG_PATH='/usr/local/hadoop/logs/*'
#DFS_PATH='/home/hduser/part-m-*'

#rm -rf  $DFS_PATH
#echo $DFS_PATH "clenup in "$HOSTNAME

for v in $HOSTS; do
    echo $DFS_PATH "clenup in "$v
    ssh -t $v "rm -rf " $DFS_PATH
    ssh -t $v "rm -rf " $LOG_PATH
done

