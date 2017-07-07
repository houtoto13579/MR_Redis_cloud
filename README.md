# MR_Redis
MR_Redis is a framework that integrates Redis, a distributed key-value store system, into MapReuce
to achieve the scalable and efficient construction of suffix array with respect to the system performance.
The draft of this research work can be found in https://arxiv.org/abs/1705.04789

## Files for MapReduce
    $ ./BioMapper.java             //Map()
    $ ./BioReducer.java            //Reduce()
    $ ./SuffixArrayRun.java        //Main program that starts the suffix array construction
    $ ./SeqNoSuffixOffset.java     //Data structure that stores the DNA sequence read
    $ ./makefile                   //It contains the information of the needed libraries during compiling
  
## Redis that supports mgetsuffix command
    The command mgetsuffix can reduce the communication overhead. 
    https://github.com/hckuo/redis/tree/add-mgetsuffix-command
## The library of Jedis that supports mgetsuffix command
    
