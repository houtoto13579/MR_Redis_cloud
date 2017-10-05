package sinica.iis;
import java.io.IOException;
import java.util.ArrayList;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

// for log4j system
import org.apache.log4j.Logger;

//redis
import redis.clients.jedis.Jedis;

public class BioMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {
  private static int method;
  static final int NUM_PREFIX = 13;

  private final Logger sLogger = Logger.getLogger(BioMapper.class.getName());
 
  private static int numNodes;
  private static String[] redisHosts;

  private ArrayList<ArrayList<String>> bulksOfKeys;

  private String[] keyMapperArray;
  private int keyCount;

  //private List<Jedis> clients = new ArrayList<Jedis>();
  /*
  private Jedis[] clients = {
    new Jedis("140.109.17.134", 6379, 300000),
    new Jedis("192.168.100.102", 6379, 300000),
    new Jedis("192.168.100.105", 6379, 300000),
    new Jedis("192.168.100.106", 6379, 300000),
    new Jedis("192.168.100.110", 6379, 300000),
    new Jedis("192.168.100.111", 6379, 300000),
    new Jedis("192.168.100.115", 6379, 300000),
    new Jedis("192.168.100.116", 6379, 300000),
    new Jedis("192.168.100.117", 6379, 300000),
    new Jedis("192.168.100.118", 6379, 300000),
    new Jedis("192.168.100.119", 6379, 300000),
    new Jedis("192.168.100.120", 6379, 300000),
    new Jedis("192.168.100.121", 6379, 300000),
    new Jedis("192.168.100.122", 6379, 300000),
    new Jedis("192.168.100.123", 6379, 300000),
    new Jedis("192.168.100.124", 6379, 300000),
  };
  */

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration job = context.getConfiguration();
    BioMapper.numNodes = job.getInt("NUM_NODES", 1);
    BioMapper.redisHosts = job.get("REDIS_HOSTS", "localhost").split(",");
    BioMapper.method = job.getInt("METHOD", 1);
    sLogger.info("The number of node is " + numNodes);
    sLogger.info("The redis hosts are " + redisHosts);
    this.bulksOfKeys = new ArrayList<ArrayList<String>>();
    for(int i = 0; i < numNodes; i++) {
      this.bulksOfKeys.add(new ArrayList<String>());
      //setting redisHosts client to prevent reset connection
      //Jedis client = new Jedis(redisHosts[i], 6379, 300000);
      //this.clients.add(client);
    }    
    //this.keyMapperArray=this.readLines("10k_key_19227");
    this.keyMapperArray=this.readLines("hdfs:/key/100k_key_39005");
    this.keyCount=this.keyMapperArray.length;
  }
  protected void cleanup(Context context) throws IOException, InterruptedException {
    for (int i = 0; i < numNodes; i++) {
      if(bulksOfKeys.get(i).size() > 0) {
    	System.out.println("redisHost: "+redisHosts[i]);
    	//System.out.print("bulksOfKeys.get: "+bulksOfKeys.get(i)); 
	Jedis client = new Jedis(redisHosts[i], 6379, 3000000);
        client.mset(bulksOfKeys.get(i).toArray(new String[0]));
        client.close();
        //clients[i].mset(bulksOfKeys.get(i).toArray(new String[0]));
      }
    }
  }
  // read key file from folder
  public String[] readLines(String filename) throws IOException {
    //FileReader fileReader = new FileReader(filename);
    
    Path pt=new Path(filename);
    FileSystem fs = FileSystem.get(new Configuration());
    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
    //BufferedReader br = new BufferedReader(fileReader);
    List<String> lines = new ArrayList<String>();
    String line = null;
    while ((line = br.readLine()) != null) {
        lines.add(line);
    }
    br.close();
    return lines.toArray(new String[lines.size()]);
  }

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    //trim spaces
    String[] result = line.split("\\s+");

    long seqNumberAndOffset;
    long seqNumber = Long.valueOf(result[0].substring(2)).longValue();
    //System.out.print("");
    StringBuilder buffer = new StringBuilder();
    buffer.append(seqNumber);

    /*** extract the first two characters and append it to seq num as the seqId ***/
    //buffer.append(result[0].charAt(1));
    buffer.append(result[0].substring(0,2));

    String seqId = buffer.toString();

    int sel = (int)(seqNumber%numNodes);
    
    bulksOfKeys.get(sel).add(seqId);
    bulksOfKeys.get(sel).add(result[1]);
        
    String suffix_str = result[1];
    seqNumberAndOffset = Long.valueOf(seqId).longValue()*1000L;

    try{
      String prefix_DNA;
      //System.out.print(suffix_str+"\r\n");
      for(int i=0;i< suffix_str.length();i++){
        prefix_DNA = suffix_str.substring(i);
        /*
        if(BioMapper.method==0)	
          context.write(new IntWritable(profilingDNASeq(prefix_DNA, NUM_PREFIX)), new LongWritable(seqNumberAndOffset+i));
        else{
          context.write(new IntWritable(profilingDNASeqByKey(prefix_DNA, NUM_PREFIX)), new LongWritable(seqNumberAndOffset+i));
        }
        */
        context.write(new IntWritable(profilingDNASeqByKey(prefix_DNA, NUM_PREFIX)), new LongWritable(seqNumberAndOffset+i));
      }
      context.write(new IntWritable(0), new LongWritable(seqNumberAndOffset+suffix_str.length()));

    } catch(IOException e){
      System.out.println("Error occurs!");
      System.out.println(e);
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
    //System.out.print(key_for_partition+"\r\n");
    return key_for_partition;
  }
  private int profilingDNASeqByKey(String seq, int num_prefix){
    
    int keyCount=this.keyCount;
    //System.out.println(keyCount);


    // we will use the bubble comparision for testing
    /*
    for(int i=0; i<keyCount; i++){
      String[] result = keyMapperArray[i].split("\\s+");
      String key = result[1];
      if(key.compareTo(seq)>=0)
        return i+1;
    }
    */
    // Binary Serch
    if(seq.equals("")||seq==""){
      return 0;
    }
    // for key 39005(Line #, not array) //
    // A: 2~24 
    // C: 11076~11154
    // G: 19526~19537
    // T: 27864~27947
    if(seq.equals("A"))
        return ThreadLocalRandom.current().nextInt(2, 23)+1;	
    if(seq.equals("C"))
        return ThreadLocalRandom.current().nextInt(11076, 11153)+1;
    if(seq.equals("G"))
        return ThreadLocalRandom.current().nextInt(19526, 19536)+1;	
    if(seq.equals("T"))
        return ThreadLocalRandom.current().nextInt(27864, 27946)+1;	
    // for key 39005(Line #, not array) //
    // AC: 3484~3501 
    // AT: 8231~8251
    // CC: 14166~14185
    // CT: 17055~17073
    // GC: 21716~21733
    // GT: 25574~25590
    // TC: 30313~30331
    // TT: 35519~35546
    if(seq.equals("AC"))
        return ThreadLocalRandom.current().nextInt(3484, 3500)+1;	
    if(seq.equals("AT"))
        return ThreadLocalRandom.current().nextInt(8231, 8250)+1;
    if(seq.equals("CC"))
        return ThreadLocalRandom.current().nextInt(14166, 14184)+1;	
    if(seq.equals("CT"))
        return ThreadLocalRandom.current().nextInt(17055, 17072)+1;	
    if(seq.equals("GC"))
        return ThreadLocalRandom.current().nextInt(21716, 21732)+1;	
    if(seq.equals("GT"))
        return ThreadLocalRandom.current().nextInt(25574, 25589)+1;	
    if(seq.equals("TC"))
        return ThreadLocalRandom.current().nextInt(30313, 30330)+1;	
    if(seq.equals("TT"))
        return ThreadLocalRandom.current().nextInt(35519, 35545)+1;	
    
    
    int upper=keyCount-1;
    int lower=0;
    int middle = (upper+lower)/2;

    String lowerKey = keyMapperArray[0].split("\\s+")[1];
    String upperKey = keyMapperArray[upper].split("\\s+")[1];
    if(lowerKey.compareTo(seq)>=0) 
      return 1;
    if(upperKey.compareTo(seq)<0)
      return keyCount+1;

    while(true){
      String middleKey = keyMapperArray[middle].split("\\s+")[1];
      if(middleKey.compareTo(seq)==0)
        return middle+1;
      else if(middleKey.compareTo(seq)>0)
        upper=middle;
      else
        lower=middle;
      middle=(upper+lower)/2;
      if (Math.abs(upper-lower)<=1){
        //System.out.println(upper+1);
        return upper+1;
      }
    }
  }
}
