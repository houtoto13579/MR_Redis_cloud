#!/bin/bash
hosts=$(cat ~/sys_sh/hosts | grep "iiscloud" | awk '{print $2}')
#hosts=$(cat /etc/hosts | grep "iiscloud" | awk '{print $2}')
for host in $hosts
do
              echo "------------------" $host "------------"
        # tachyon
        rsync -avz --delete --exclude 'log/*' -e 'ssh -o StrictHostKeyChecking=no' /usr/local/flink/  $host:/usr/local/flink/

        # spark
        #rsync -avz --delete --exclude 'logs/*' --exclude 'pids/*' --exclude "work/*" -e 'ssh -o StrictHostKeyChecking=no' ~/spark/  $host:spark/

done
