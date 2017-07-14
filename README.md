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
### Install project dependencies
`mvn clean install`
### Build Project into jar
`mvn clean package`
> The generated jar is located in ${project.basdir}/target/${artifactId}-${version}-jar-with-dependencies.jar
## Hadoop
### Run
`hadoop jar ${artifactId}-${version}.jar ${job-name} ${input-folder} ${output-folder}`
> This command above take files from ${input-folder} and generate the result into $output-folder}.

### Remove result
`hadoop fs -rm -r ${output-folder}` 

## Files for MapReduce
- BioMapper.java             // Map()
- BioReducer.java            //Reduce()
- BioPartitioner.java        //Partition of the key space: not automatic yet
- SuffixArrayRun.java        //Main program that starts the suffix array construction
- SeqNoSuffixOffset.java     //Data structure that stores the DNA sequence read
  
## Redis that supports mgetsuffix command
    You need to install Redis on the nodes that can serve the key-value access.
    The command mgetsuffix can reduce the communication overhead. 
    https://github.com/hckuo/redis/tree/add-mgetsuffix-command
## The library of Jedis that supports mgetsuffix command
    This is the client library that helps the mappers and reducers to communicate with Redis.
    https://github.com/hckuo/jedis/tree/add-mgetsuffix-command
    
