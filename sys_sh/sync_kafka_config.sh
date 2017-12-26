#!/bin/bash
hosts=$(cat ~/sys_sh/hosts | grep "iiscloud" | awk '{print $2}')
#hosts=$(cat /etc/hosts | grep "iiscloud" | awk '{print $2}')
for host in $hosts
do
              echo "------------------" $host "------------"
        # tachyon
        rsync -avz --delete --exclude 'logs/*' -e 'ssh -o StrictHostKeyChecking=no' /usr/local/kafka/  $host:/usr/local/kafka/

        # spark
        #rsync -avz --delete --exclude 'logs/*' --exclude 'pids/*' --exclude "work/*" -e 'ssh -o StrictHostKeyChecking=no' ~/spark/  $host:spark/

done
