package sinica.iis;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
// for log4j system
import org.apache.log4j.Logger;

//redis
import redis.clients.jedis.Jedis;

public class BioMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {
  static final int NUM_PREFIX = 13;

  private final Logger sLogger = Logger.getLogger(BioMapper.class.getName());
  // private static int numNodes = 16; 
  // private static String[] jedisHosts = {"140.109.17.134"
  //     , "192.168.100.102", "192.168.100.112", "192.168.100.105", "192.168.100.106", "192.168.100.107", "192.168.100.118", "192.168.100.109", "192.168.100.110", "192.168.100.111"
  //     , "192.168.100.119", "192.168.100.113", "192.168.100.121", "192.168.100.115", "192.168.100.116", "192.168.100.117"};
 
  private static int numNodes = 3;
  private static String[] jedisHosts = {"slave1", "slave2", "slave3"};

  private ArrayList<ArrayList<String>> bulksOfKeys;


  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    if(System.getProperty("JedisHosts") != null && System.getProperty("NumNodes") != null) {
      String[] hosts = System.getProperty("JedisHosts").split(",");
      Integer numNodes = Integer.valueOf(System.getProperty("NumNodes"));
      BioMapper.jedisHosts = hosts;
      BioMapper.numNodes = numNodes;
      sLogger.info("Found specification of jedis hosts: " + hosts.toString() + " and number of nodes: " + numNodes + " .");
    } else {
      sLogger.info("No specification of jedis hosts and number of nodes, using in-code config.");
    }
    this.bulksOfKeys = new ArrayList<ArrayList<String>>();
  	  for(int i = 0; i < numNodes; i++) {
  	    this.bulksOfKeys.add(new ArrayList<String>());
  	  }
  }


  protected void cleanup(Context context) throws IOException, InterruptedException {
    for (int i = 0; i < numNodes; i++) {
      if(bulksOfKeys.get(i).size() > 0) {
        new Jedis(jedisHosts[i], 6379, 300000).mset(bulksOfKeys.get(i).toArray(new String[0]));
      }
    }
  }


  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    //trim spaces
    String[] result = line.split("\\s+");

    long seqNumberAndOffset;
    long seqNumber = Long.valueOf(result[0].substring(2)).longValue();

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

}