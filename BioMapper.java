import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

// for log4j system
import org.apache.log4j.Logger;

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.URI;
import org.apache.hadoop.filecache.DistributedCache;

//memcached
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import java.net.InetSocketAddress;
import java.lang.IllegalStateException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

//redis
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class BioMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {
  static final int NUM_PREFIX = 13;

  private static final Logger sLogger = Logger.getLogger(BioMapper.class.getName());

  //private JedisPool pool_0;
  //private JedisPool pool_1;
  //private JedisPool pool_2;
  //private JedisPool pool_3;
  //private JedisPool pool_4;
  //private JedisPool pool_5;
  //private JedisPool pool_6;
  //private JedisPool pool_7;
  //private JedisPool pool_8;
  //private JedisPool pool_9;
  //private JedisPool pool_10;
  //private JedisPool pool_11;
  //private JedisPool pool_12;
  //private JedisPool pool_13;
  //private JedisPool pool_14;
  //private JedisPool pool_15;

  private ArrayList <String> bulkOfKeys_0;
  private ArrayList <String> bulkOfKeys_1;
  private ArrayList <String> bulkOfKeys_2;
  private ArrayList <String> bulkOfKeys_3;
  private ArrayList <String> bulkOfKeys_4;
  private ArrayList <String> bulkOfKeys_5;
  private ArrayList <String> bulkOfKeys_6;
  private ArrayList <String> bulkOfKeys_7;
  private ArrayList <String> bulkOfKeys_8;
  private ArrayList <String> bulkOfKeys_9;
  private ArrayList <String> bulkOfKeys_10;
  private ArrayList <String> bulkOfKeys_11;
  private ArrayList <String> bulkOfKeys_12;
  private ArrayList <String> bulkOfKeys_13;
  private ArrayList <String> bulkOfKeys_14;
  private ArrayList <String> bulkOfKeys_15;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    this.bulkOfKeys_0 = new ArrayList<String>();
    this.bulkOfKeys_1 = new ArrayList<String>();
    this.bulkOfKeys_2 = new ArrayList<String>();
    this.bulkOfKeys_3 = new ArrayList<String>();
    this.bulkOfKeys_4 = new ArrayList<String>();
    this.bulkOfKeys_5 = new ArrayList<String>();
    this.bulkOfKeys_6 = new ArrayList<String>();
    this.bulkOfKeys_7 = new ArrayList<String>();
    this.bulkOfKeys_8 = new ArrayList<String>();
    this.bulkOfKeys_9 = new ArrayList<String>();
    this.bulkOfKeys_10 = new ArrayList<String>();
    this.bulkOfKeys_11 = new ArrayList<String>();
    this.bulkOfKeys_12 = new ArrayList<String>();
    this.bulkOfKeys_13 = new ArrayList<String>();
    this.bulkOfKeys_14 = new ArrayList<String>();
    this.bulkOfKeys_15 = new ArrayList<String>();

  }


  protected void cleanup(Context context) throws IOException, InterruptedException {
    Jedis jedis0  = new Jedis("140.109.17.134", 6379, 300000);
    Jedis jedis1  = new Jedis("192.168.100.102", 6379, 300000);
    Jedis jedis2  = new Jedis("192.168.100.112", 6379, 300000);
    Jedis jedis3  = new Jedis("192.168.100.105", 6379, 300000);
    Jedis jedis4  = new Jedis("192.168.100.106", 6379, 300000);
    Jedis jedis5  = new Jedis("192.168.100.107", 6379, 300000);
    Jedis jedis6  = new Jedis("192.168.100.118", 6379, 300000);
    Jedis jedis7  = new Jedis("192.168.100.109", 6379, 300000);
    Jedis jedis8  = new Jedis("192.168.100.110", 6379, 300000);
    Jedis jedis9  = new Jedis("192.168.100.111", 6379, 300000);
    Jedis jedis10 = new Jedis("192.168.100.119", 6379, 300000);
    Jedis jedis11 = new Jedis("192.168.100.113", 6379, 300000);
    Jedis jedis12 = new Jedis("192.168.100.121", 6379, 300000);
    Jedis jedis13 = new Jedis("192.168.100.115", 6379, 300000);
    Jedis jedis14 = new Jedis("192.168.100.116", 6379, 300000);
    Jedis jedis15 = new Jedis("192.168.100.117", 6379, 300000);

    jedis0.mset(bulkOfKeys_0.toArray(new String[0]));
    jedis1.mset(bulkOfKeys_1.toArray(new String[0]));
    jedis2.mset(bulkOfKeys_2.toArray(new String[0]));
    jedis3.mset(bulkOfKeys_3.toArray(new String[0]));
    jedis4.mset(bulkOfKeys_4.toArray(new String[0]));
    jedis5.mset(bulkOfKeys_5.toArray(new String[0]));
    jedis6.mset(bulkOfKeys_6.toArray(new String[0]));
    jedis7.mset(bulkOfKeys_7.toArray(new String[0]));
    jedis8.mset(bulkOfKeys_8.toArray(new String[0]));
    jedis9.mset(bulkOfKeys_9.toArray(new String[0]));
    jedis10.mset(bulkOfKeys_10.toArray(new String[0]));
    jedis11.mset(bulkOfKeys_11.toArray(new String[0]));
    jedis12.mset(bulkOfKeys_12.toArray(new String[0]));
    jedis13.mset(bulkOfKeys_13.toArray(new String[0]));
    jedis14.mset(bulkOfKeys_14.toArray(new String[0]));
    jedis15.mset(bulkOfKeys_15.toArray(new String[0]));
  }


  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] result = line.split("\\s+");

    long seqNumberAndOffset;
    long seqNumber = Long.valueOf(result[0].substring(2)).longValue();

    StringBuilder buffer = new StringBuilder();
    buffer.append(seqNumber);

    /*** extract the first two characters and append it to seq num as the seqId ***/
    //buffer.append(result[0].charAt(1));
    buffer.append(result[0].substring(0,2));

    String seqId = buffer.toString();

    int sel = (int)(seqNumber%16L);


    switch(sel){
      case 1:  bulkOfKeys_1.add(seqId); bulkOfKeys_1.add(result[1]);break;
      case 2:  bulkOfKeys_2.add(seqId); bulkOfKeys_2.add(result[1]);break;
      case 3:  bulkOfKeys_3.add(seqId); bulkOfKeys_3.add(result[1]);break;
      case 4:  bulkOfKeys_4.add(seqId); bulkOfKeys_4.add(result[1]);break;
      case 5:  bulkOfKeys_5.add(seqId); bulkOfKeys_5.add(result[1]);break;
      case 6:  bulkOfKeys_6.add(seqId); bulkOfKeys_6.add(result[1]);break;
      case 7:  bulkOfKeys_7.add(seqId); bulkOfKeys_7.add(result[1]);break;
      case 8:  bulkOfKeys_8.add(seqId); bulkOfKeys_8.add(result[1]);break;
      case 9:  bulkOfKeys_9.add(seqId); bulkOfKeys_9.add(result[1]);break;
      case 10: bulkOfKeys_10.add(seqId); bulkOfKeys_10.add(result[1]);break;
      case 11: bulkOfKeys_11.add(seqId); bulkOfKeys_11.add(result[1]);break;
      case 12: bulkOfKeys_12.add(seqId); bulkOfKeys_12.add(result[1]);break;
      case 13: bulkOfKeys_13.add(seqId); bulkOfKeys_13.add(result[1]);break;
      case 14: bulkOfKeys_14.add(seqId); bulkOfKeys_14.add(result[1]);break;
      case 15: bulkOfKeys_15.add(seqId); bulkOfKeys_15.add(result[1]);break;
      default: bulkOfKeys_0.add(seqId); bulkOfKeys_0.add(result[1]);break;
    }

    
    String suffix_str = result[1];
    seqNumberAndOffset = Long.valueOf(seqId).longValue()*1000L;

    try{
      String prefix_DNA;

      for(int i=0;i< suffix_str.length();i++){
        prefix_DNA = suffix_str.substring(i);
        context.write(new IntWritable(profilingDNASeq(prefix_DNA, NUM_PREFIX)), new LongWritable(seqNumberAndOffset+i));
      }
      context.write(new IntWritable(0), new LongWritable(seqNumberAndOffset+suffix_str.length()));

    } catch(IOException e){
      System.out.println("Error occurs!");
    }


  }

  private int profilingDNASeq(String seq, int num_prefix){
    int key_for_partition = 0;

    for(int i=0;i< Math.min(num_prefix, seq.length());i++){
      switch(seq.charAt(i)){
        case 'A': key_for_partition += Math.pow(5, num_prefix-1-i); break;
        case 'C': key_for_partition += 2*Math.pow(5,  num_prefix-1-i); break;
        case 'G': key_for_partition += 3*Math.pow(5,  num_prefix-1-i); break;
        case 'T': key_for_partition += 4*Math.pow(5,  num_prefix-1-i); break;
        default: break;
      }
    }

    return key_for_partition;
  }

  private int encodeDNASeqInDiffGrain(String seq, int short_prefix, int long_prefix){
    int key_for_partition = 0;

    for(int i=0;i< Math.min(short_prefix, seq.length());i++){
      switch(seq.charAt(i)){
        case 'A': key_for_partition += Math.pow(5, long_prefix-1-i); break;
        case 'C': key_for_partition += 2*Math.pow(5,  long_prefix-1-i); break;
        case 'G': key_for_partition += 3*Math.pow(5,  long_prefix-1-i); break;
        case 'T': key_for_partition += 4*Math.pow(5,  long_prefix-1-i); break;
        default: break;
      }
    }

    return key_for_partition;
  }

}
