# MR_Redis
MR_Redis is a framework that integrates Redis, a distributed key-value store system, into MapReuce
to achieve the scalable and efficient construction of suffix array with respect to the system performance.
The draft of this research work can be found in https://arxiv.org/abs/1705.04789

## Maven Dependency Manager
### Maven Installation
```
sudo apt-get update
sudo apt-get install maven
```
### Third Party Installation
Because we will use customize redis, we will need to install third-party jedis.
```shell
mvn install:install-file -Dfile=libs/jedis-3.0.0-SNAPSHOT.jar -DgroupID=redis.clients -DartifactId=jedis -Dversion=3.0.0-SNAPSHOT -Dpackaging=jar
```
### Install project dependencies
`mvn clean install`
### Build Project into jar
`mvn clean package`
> The generated jar is located in ${project.basdir}/target/${artifactId}-${version}-jar-with-dependencies.jar
## Hadoop
### Environment setting
1. Java version 1.8 (1.7 will cause problem in Mapper)
2. Hadoop version 2.7.2

### Start Proxy Server and History(8088)
```shell
start-yarn.sh
start-dfs.sh
mr-jobhistory-daemon.sh --config $HADOOP_HOME/etc/hadoop/ start historyserver
yarn-daemon.sh start proxyserver
```

## Dir and File

- src (Main code for proposed method)
- teraSort (using teraSort to GSA construction)
- sys_sh (checking system and flush Redis)
- target (java compile)
- libs (java compile)
- keys (keys for proposed method, contain fast index and key file)
- logs (get data from 50070 and 8088 for experiment)
- validate.py (validation the output file)
- merge.py (merge output file)
- generate_index.py (fast index)
- newkey.py (generate new key file from old the output of reducer)

## Files for MapReduce(under src directory)
- BioMapper.java             //Map()
- BioReducer.java            //Reduce()
- BioPartitioner.java        //Partition of the key space
- SuffixArrayRun.java        //Main program that starts the suffix array construction
- SeqNoSuffixOffset.java     //Data structure that stores the DNA sequence read

## Execution

before execution, use flush_redis.sh first to clean the Redis database.

### Execution on Local
```shell
mvn clean package && hadoop fs -rm -r -f ~/output_TEST && hadoop jar target/MR_Redis-1.0-SNAPSHOT-jar-with-dependencies.jar sinica.iis.SuffixArrayRun ~/input_10K ~/output_TEST
```
### Execution on cloud
```shell
mvn clean package && hadoop fs -rm -r -f /output_10K_grouper && hadoop jar target/MR_Redis-1.0-SNAPSHOT-jar-with-dependencies.jar sinica.iis.SuffixArrayRun /input_10K_grouper /output_10K_grouper
```

## Validating(Testing)

Change the directory in validate.py before using it.
```shell
    python validate.py
```
However, the sequence made by HH is different, which 63 is the smallest.

## Web UI
Web UI can access to the log, but the historyUI link is wrong. Just replace iiscloud01 with IP.

You can access Web UI by two port.
- DFS: port 50070
- yarn: port 8088

## Slice File
head -n 60000000 LGC_EZ01_400bp_AGTTCC_L001.R1.sfa | tail -n 30000000 > ~/chunk2.sfa  <br />
head -n 30000000 eel_PE400.R1.sfa > eel_chunk1.sfa

## TeraSort

There are two steps in performing TeraSort.

First, teraGen in teraSort/gen
```shell
yarn jar SuffixGen.jar SuffixGen <input> <output>
```
Next, teraSort in teraSort/sort
```shell
yarn jar SuffixSort.jar testHdp.SuffixSort -Dmapred.reduce.tasks=64 <input> <output>
```

## Troubleshooting
### Balancer
Hadoop(HDFS) will encounter inbalanced problems while storing file or in the middle of map and reduce.
You can run balancer to solve the problem.
```shell
hadoop balancer [-threshold <t>] 
```

### Cluster Problem
#### Safe mode
There are many probelms which will cause the nodemanager to enter safemode. You cannot use hadoop or access data then. Leave safe mode by excuting this:
```shell
hadoop dfsadmin -safemode leave
```
***However***, you should check the node condition before leaving save mode.
#### Node break down
You can try restarting that node by stop and start the nodemanager and datanode.
```shell
hadoop-daemon.sh start datanode
yarn-daemon.sh start nodemanager
```
If problem remain, try restart the whole computer.

Also, if some node is failed and causing a corrupted file, use this to clean these corrupted file:
```shell
hdfs fsck / -delete
```
You might need to leave safe node or restart hdfs system to execute this command.

#### Format
If problem remain, you could format the whole system.
If you format NameNode only, you have to change the cluster ID in /hadoop_disk/name/current/Version to the one in data node.
In addition, format the NameNode won't clean the file in each DataNodes. You need to rm each dir(hadoop_disk) in data nodes to release the disk space.  

#### Node don't have enough space and become unhealthy node
You can use balancer to solve that problem

### Redis server problem

It is recommended that Redis need to be flush every time before use:
```shell
redis-cli flushall
```
which can be completed automatically using flush_redis.sh under sys_sh directory

#### Memory not enough
change configuration in /etc/redis/redis.conf

#### restart or start redis server by yourself
1. Copy compiled redis from /usr/bin/redis* to new node's /usr/bin/
2. change node in /sys_sh/hosts, and execute flush_redis.sh under sys_sh, which will perform flush and restart all redis
3. or use following command
```shell
ssh -t iiscloudxx "sudo /etc/init.d/redis-server start"
```

### Useful link
1. [Datanode & Namenode Setting (in Chinese)](http://puremonkey2010.blogspot.tw/2013/10/hadoop-linux-1-namenode-2-datanode.html)
2. [Tuning Yarn](https://www.cloudera.com/documentation/enterprise/5-8-x/topics/cdh_ig_yarn_tuning.html)

### Redis that supports mgetsuffix command
    You need to install Redis on the nodes that can serve the key-value access.
    The command mgetsuffix can reduce the communication overhead. 
    https://github.com/hckuo/redis/tree/add-mgetsuffix-command
### The library of Jedis that supports mgetsuffix command
    This is the client library that helps the mappers and reducers to communicate with Redis.
    https://github.com/hckuo/jedis/tree/add-mgetsuffix-command
