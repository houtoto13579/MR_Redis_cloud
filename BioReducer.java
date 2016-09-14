import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

// for log4j system
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

//memcached
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import java.net.InetSocketAddress;


//redis
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;


public class BioReducer extends Reducer<IntWritable, LongWritable, LongWritable, Text> {
  static final int NUM_PREFIX = 13;
  static final int GROUP_SIZE = 1600000;
  static final int HASH_CAPACITY = 1100000;

  private static final Logger sLogger = Logger.getLogger(BioReducer.class.getName());


  private JedisPool pool_0;
  private JedisPool pool_1;
  private JedisPool pool_2;
  private JedisPool pool_3;
  private JedisPool pool_4;
  private JedisPool pool_5;
  private JedisPool pool_6;
  private JedisPool pool_7;
  private JedisPool pool_8;
  private JedisPool pool_9;
  private JedisPool pool_10;
  private JedisPool pool_11;
  private JedisPool pool_12;
  private JedisPool pool_13;
  private JedisPool pool_14;
  private JedisPool pool_15;
 
  private JedisPoolConfig jpc_0 = new JedisPoolConfig();
  private JedisPoolConfig jpc_1 = new JedisPoolConfig();
  private JedisPoolConfig jpc_2 = new JedisPoolConfig();
  private JedisPoolConfig jpc_3 = new JedisPoolConfig();
  private JedisPoolConfig jpc_4 = new JedisPoolConfig();
  private JedisPoolConfig jpc_5 = new JedisPoolConfig();
  private JedisPoolConfig jpc_6 = new JedisPoolConfig();
  private JedisPoolConfig jpc_7 = new JedisPoolConfig();
  private JedisPoolConfig jpc_8 = new JedisPoolConfig();
  private JedisPoolConfig jpc_9 = new JedisPoolConfig();
  private JedisPoolConfig jpc_10 = new JedisPoolConfig();
  private JedisPoolConfig jpc_11 = new JedisPoolConfig();
  private JedisPoolConfig jpc_12 = new JedisPoolConfig();
  private JedisPoolConfig jpc_13 = new JedisPoolConfig();
  private JedisPoolConfig jpc_14 = new JedisPoolConfig();
  private JedisPoolConfig jpc_15 = new JedisPoolConfig();

  private Jedis jedis0;
  private Jedis jedis1;
  private Jedis jedis2;
  private Jedis jedis3;
  private Jedis jedis4;
  private Jedis jedis5;
  private Jedis jedis6;
  private Jedis jedis7;
  private Jedis jedis8;
  private Jedis jedis9;
  private Jedis jedis10;
  private Jedis jedis11;
  private Jedis jedis12;
  private Jedis jedis13;
  private Jedis jedis14;
  private Jedis jedis15;

  private Pipeline pipeline_0;
  private Pipeline pipeline_1;
  private Pipeline pipeline_2;
  private Pipeline pipeline_3;
  private Pipeline pipeline_4;
  private Pipeline pipeline_5;
  private Pipeline pipeline_6;
  private Pipeline pipeline_7;
  private Pipeline pipeline_8;
  private Pipeline pipeline_9;
  private Pipeline pipeline_10;
  private Pipeline pipeline_11;
  private Pipeline pipeline_12;
  private Pipeline pipeline_13;
  private Pipeline pipeline_14;
  private Pipeline pipeline_15;


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

  private List <String> bulkOfValues_0;
  private List <String> bulkOfValues_1;
  private List <String> bulkOfValues_2;
  private List <String> bulkOfValues_3;
  private List <String> bulkOfValues_4;
  private List <String> bulkOfValues_5;
  private List <String> bulkOfValues_6;
  private List <String> bulkOfValues_7;
  private List <String> bulkOfValues_8;
  private List <String> bulkOfValues_9;
  private List <String> bulkOfValues_10;
  private List <String> bulkOfValues_11;
  private List <String> bulkOfValues_12;
  private List <String> bulkOfValues_13;
  private List <String> bulkOfValues_14;
  private List <String> bulkOfValues_15;



  private ArrayList <Integer> scramble_order;

  //private Map <String, StringBuilder> recordToSubIndex_tbl = new HashMap<String, StringBuilder>();
  private Map <Long, StringBuilder> recordToSubIndex_tbl;
  private int get_size;

  private LongWritable seqNumber;
  private Text suffixOffset;
 
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    
    this.jpc_0.setMaxTotal(100);
    this.jpc_1.setMaxTotal(100);
    this.jpc_2.setMaxTotal(100);
    this.jpc_3.setMaxTotal(100);
    this.jpc_4.setMaxTotal(100);
    this.jpc_5.setMaxTotal(100);
    this.jpc_6.setMaxTotal(100);
    this.jpc_7.setMaxTotal(100);
    this.jpc_8.setMaxTotal(100);
    this.jpc_9.setMaxTotal(100);
    this.jpc_10.setMaxTotal(100);
    this.jpc_11.setMaxTotal(100);
    this.jpc_12.setMaxTotal(100);
    this.jpc_13.setMaxTotal(100);
    this.jpc_14.setMaxTotal(100);
    this.jpc_15.setMaxTotal(100);


    this.pool_0  = new JedisPool(this.jpc_0, "140.109.17.134", 6379, 900000);
    this.pool_1  = new JedisPool(this.jpc_1, "192.168.100.102", 6379, 900000);
    this.pool_2  = new JedisPool(this.jpc_2, "192.168.100.112", 6379, 900000);
    this.pool_3  = new JedisPool(this.jpc_3, "192.168.100.105", 6379, 900000);
    this.pool_4  = new JedisPool(this.jpc_4, "192.168.100.106", 6379, 900000);
    this.pool_5  = new JedisPool(this.jpc_5, "192.168.100.107", 6379, 900000);
    this.pool_6  = new JedisPool(this.jpc_6, "192.168.100.118", 6379, 900000);
    this.pool_7  = new JedisPool(this.jpc_7, "192.168.100.109", 6379, 900000);
    this.pool_8  = new JedisPool(this.jpc_8, "192.168.100.110", 6379, 900000);
    this.pool_9  = new JedisPool(this.jpc_9, "192.168.100.111", 6379, 900000);
    this.pool_10 = new JedisPool(this.jpc_10, "192.168.100.119", 6379, 900000);
    this.pool_11 = new JedisPool(this.jpc_11, "192.168.100.113", 6379, 900000);
    this.pool_12 = new JedisPool(this.jpc_12, "192.168.100.114", 6379, 900000);
    this.pool_13 = new JedisPool(this.jpc_13, "192.168.100.115", 6379, 900000);
    this.pool_14 = new JedisPool(this.jpc_14, "192.168.100.116", 6379, 900000);
    this.pool_15 = new JedisPool(this.jpc_15, "192.168.100.117", 6379, 900000);

    this.jedis0 = this.pool_0.getResource();
    this.jedis1 = this.pool_1.getResource();
    this.jedis2 = this.pool_2.getResource();
    this.jedis3 = this.pool_3.getResource();
    this.jedis4 = this.pool_4.getResource();
    this.jedis5 = this.pool_5.getResource();
    this.jedis6 = this.pool_6.getResource();
    this.jedis7 = this.pool_7.getResource();
    this.jedis8 = this.pool_8.getResource();
    this.jedis9 = this.pool_9.getResource();
    this.jedis10 = this.pool_10.getResource();
    this.jedis11 = this.pool_11.getResource();
    this.jedis12 = this.pool_12.getResource();
    this.jedis13 = this.pool_13.getResource();
    this.jedis14 = this.pool_14.getResource();
    this.jedis15 = this.pool_15.getResource();

    this.pipeline_0 = this.jedis0.pipelined();
    this.pipeline_1 = this.jedis1.pipelined();
    this.pipeline_2 = this.jedis2.pipelined();
    this.pipeline_3 = this.jedis3.pipelined();
    this.pipeline_4 = this.jedis4.pipelined();
    this.pipeline_5 = this.jedis5.pipelined();
    this.pipeline_6 = this.jedis6.pipelined();
    this.pipeline_7 = this.jedis7.pipelined();
    this.pipeline_8 = this.jedis8.pipelined();
    this.pipeline_9 = this.jedis9.pipelined();
    this.pipeline_10 = this.jedis10.pipelined();
    this.pipeline_11 = this.jedis11.pipelined();
    this.pipeline_12 = this.jedis12.pipelined();
    this.pipeline_13 = this.jedis13.pipelined();
    this.pipeline_14 = this.jedis14.pipelined();
    this.pipeline_15 = this.jedis15.pipelined();


    this.get_size = 0;
    this.seqNumber = new LongWritable();
    this.suffixOffset = new Text();
    this.recordToSubIndex_tbl = new HashMap<Long, StringBuilder>(HASH_CAPACITY);

    this.scramble_order = new  ArrayList <Integer>(16);
    for(int i=0;i<16;i++)
      this.scramble_order.add(new Integer(i));

  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    if(this.recordToSubIndex_tbl.size() > 0){
      batchProcess(context);
      

      this.pool_0.destroy();
      this.pool_1.destroy();
      this.pool_2.destroy();
      this.pool_3.destroy();
      this.pool_4.destroy();
      this.pool_5.destroy();
      this.pool_6.destroy();
      this.pool_7.destroy();
      this.pool_8.destroy();
      this.pool_9.destroy();
      this.pool_10.destroy();
      this.pool_11.destroy();
      this.pool_12.destroy();
      this.pool_13.destroy();
      this.pool_14.destroy();
      this.pool_15.destroy();
    }
  }

    @Override
    public void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    //public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

      long start = System.currentTimeMillis();
      long end;

      int sel;
      int offset;
      //String mem_key;
      Long mem_key;
      String decoded_prefix;

      int reduce_group_size = 0;
      
      if(key.get() == 0){
        StringBuilder tmp_suffix_offset = new StringBuilder("$ ");;
        for(LongWritable value: values){
          offset = (int)(value.get()%1000L);
          tmp_suffix_offset.append(offset);
          //context.write(new LongWritable((value.get()-offset)/1000L), new Text(tmp_suffix_offset.toString()));
          this.seqNumber.set((value.get()-offset)/1000L);
          this.suffixOffset.set(tmp_suffix_offset.toString());
          context.write(this.seqNumber, this.suffixOffset);

          tmp_suffix_offset.delete(2, tmp_suffix_offset.length());
          reduce_group_size++;
        }
      }
      //else if(key.get() <= 390620 && key.get()%5 == 0){
      //else if(key.get() <= 9765620 && key.get()%5 == 0){
      //else if(normal_case && key.get() <= 244140620 && key.get()%5 == 0){
      //else if(key.get() <= 244140620 && key.get()%5 == 0){
      //else if(key.get() <= 1220703120 && key.get()%5 == 0){
      //else if(key.get() <= 48828120 && key.get()%5 == 0){
      //else if(key.get() <= 3120 && key.get()%5 == 0){
      //else if(key.get()%625 == 0 && isLargeGrain(key.get()) ){
      else if(isLargeGrain(key.get()) ){
        /*** process the accumulated suffixes to preserve the order ***/
        if(this.get_size > 0){
          batchProcess(context); 
          this.get_size = 0;
        }


        if(key.get()%625 == 0){
       
          decoded_prefix = decodePrefix(key.get(), NUM_PREFIX);

          StringBuilder tmp_suffix_offset = new StringBuilder(decoded_prefix);;
          for(LongWritable value: values){
            offset = (int)(value.get()%1000L);
            tmp_suffix_offset.append(" ");
            tmp_suffix_offset.append(offset);

            //context.write(new LongWritable((value.get()-offset)/1000L), new Text(tmp_suffix_offset.toString()));
            this.seqNumber.set((value.get()-offset)/1000L);
            this.suffixOffset.set(tmp_suffix_offset.toString());
            context.write(this.seqNumber, this.suffixOffset);

            tmp_suffix_offset.delete(decoded_prefix.length(), tmp_suffix_offset.length());
            reduce_group_size++;
          }
        }
        else{
          StringBuilder suffix_offset;

          for(LongWritable value: values){
            offset = (int)(value.get()%1000L);
            //mem_key = Long.toString((value.get()-offset)/1000L);
            mem_key = new Long((value.get()-offset)/1000L);
            
            if(this.recordToSubIndex_tbl.containsKey(mem_key)){
              suffix_offset = this.recordToSubIndex_tbl.get(mem_key);
              suffix_offset.append("-");
              suffix_offset.append(offset);
              this.recordToSubIndex_tbl.put(mem_key, suffix_offset);
            }
            else{
              suffix_offset = new StringBuilder(Integer.toString(offset));
              this.recordToSubIndex_tbl.put(mem_key, suffix_offset);
            }

            reduce_group_size++;
            this.get_size++;
          }


                  
          if(this.get_size > GROUP_SIZE){
            batchProcess(context);
            this.get_size = 0;
          }
          
          //outlier of reduce group
          //if(reduce_group_size > GROUP_SIZE)
          //  sLogger.info("Key "+decodePrefix(key.get(), NUM_PREFIX)+"for Reduce group size: "+reduce_group_size);

        }

      }
      else if(key.get()%78125== 0){

        /*** process the accumulated suffixes to preserve the order ***/
        if(this.get_size > 0){
          batchProcess(context);
          this.get_size = 0;
        }
       
        decoded_prefix = decodePrefix(key.get(), NUM_PREFIX);

        StringBuilder tmp_suffix_offset = new StringBuilder(decoded_prefix);;
        for(LongWritable value: values){
          offset = (int)(value.get()%1000L);
          tmp_suffix_offset.append(" ");
          tmp_suffix_offset.append(offset);

          //context.write(new LongWritable((value.get()-offset)/1000L), new Text(tmp_suffix_offset.toString()));
          this.seqNumber.set((value.get()-offset)/1000L);
          this.suffixOffset.set(tmp_suffix_offset.toString());
          context.write(this.seqNumber, this.suffixOffset);

          tmp_suffix_offset.delete(decoded_prefix.length(), tmp_suffix_offset.length());
          reduce_group_size++;
        }

      }

      else{
        StringBuilder suffix_offset;

        for(LongWritable value: values){
          offset = (int)(value.get()%1000L);
          //mem_key = Long.toString((value.get()-offset)/1000L);
          mem_key = new Long((value.get()-offset)/1000L);
          
          if(this.recordToSubIndex_tbl.containsKey(mem_key)){
            suffix_offset = this.recordToSubIndex_tbl.get(mem_key);
            suffix_offset.append("-");
            suffix_offset.append(offset);
            this.recordToSubIndex_tbl.put(mem_key, suffix_offset);
          }
          else{
            suffix_offset = new StringBuilder(Integer.toString(offset));
            this.recordToSubIndex_tbl.put(mem_key, suffix_offset);
          }

          reduce_group_size++;
          this.get_size++;
        }


                
        if(this.get_size > GROUP_SIZE){
          batchProcess(context);
          this.get_size = 0;
        }
        
        //outlier of reduce group
        //if(reduce_group_size > GROUP_SIZE)
        //  sLogger.info("Key "+decodePrefix(key.get(), NUM_PREFIX)+"for Reduce group size: "+reduce_group_size);
        

      }
      end = System.currentTimeMillis();      
      //sLogger.info("One Reduce group time("+decodePrefix(key.get(), NUM_PREFIX)+"): "+(end-start)+" ms");
      //sLogger.info("                 size: "+reduce_group_size);
        

    }//end of Reduce()


    private void batchProcess(Context context)throws IOException, InterruptedException{
      int sel;
      int bulkOfKeys_0_size = 0;
      int bulkOfKeys_1_size = 0;
      int bulkOfKeys_2_size = 0;
      int bulkOfKeys_3_size = 0;
      int bulkOfKeys_4_size = 0;
      int bulkOfKeys_5_size = 0;
      int bulkOfKeys_6_size = 0;
      int bulkOfKeys_7_size = 0;
      int bulkOfKeys_8_size = 0;
      int bulkOfKeys_9_size = 0;
      int bulkOfKeys_10_size = 0;
      int bulkOfKeys_11_size = 0;
      int bulkOfKeys_12_size = 0;
      int bulkOfKeys_13_size = 0;
      int bulkOfKeys_14_size = 0;
      int bulkOfKeys_15_size = 0;

      for(Long seq : this.recordToSubIndex_tbl.keySet()){
        //sel = (int)((Long.valueOf(seq).longValue()/10L)%16L);
        sel = (int)((seq.longValue()/10L)%16L);

        switch(sel){
          case 1:  bulkOfKeys_1_size++; break;
          case 2:  bulkOfKeys_2_size++; break;
          case 3:  bulkOfKeys_3_size++; break;
          case 4:  bulkOfKeys_4_size++; break;
          case 5:  bulkOfKeys_5_size++; break;
          case 6:  bulkOfKeys_6_size++; break;
          case 7:  bulkOfKeys_7_size++; break;
          case 8:  bulkOfKeys_8_size++; break;
          case 9:  bulkOfKeys_9_size++; break;
          case 10: bulkOfKeys_10_size++; break;
          case 11: bulkOfKeys_11_size++; break;
          case 12: bulkOfKeys_12_size++; break;
          case 13: bulkOfKeys_13_size++; break;
          case 14: bulkOfKeys_14_size++; break;
          case 15: bulkOfKeys_15_size++; break;
          default: bulkOfKeys_0_size++; break;
        }
      }


      this.bulkOfKeys_0 = new ArrayList<String>(bulkOfKeys_0_size);
      this.bulkOfKeys_1 = new ArrayList<String>(bulkOfKeys_1_size);
      this.bulkOfKeys_2 = new ArrayList<String>(bulkOfKeys_2_size);
      this.bulkOfKeys_3 = new ArrayList<String>(bulkOfKeys_3_size);
      this.bulkOfKeys_4 = new ArrayList<String>(bulkOfKeys_4_size);
      this.bulkOfKeys_5 = new ArrayList<String>(bulkOfKeys_5_size);
      this.bulkOfKeys_6 = new ArrayList<String>(bulkOfKeys_6_size);
      this.bulkOfKeys_7 = new ArrayList<String>(bulkOfKeys_7_size);
      this.bulkOfKeys_8 = new ArrayList<String>(bulkOfKeys_8_size);
      this.bulkOfKeys_9 = new ArrayList<String>(bulkOfKeys_9_size);
      this.bulkOfKeys_10 = new ArrayList<String>(bulkOfKeys_10_size);
      this.bulkOfKeys_11 = new ArrayList<String>(bulkOfKeys_11_size);
      this.bulkOfKeys_12 = new ArrayList<String>(bulkOfKeys_12_size);
      this.bulkOfKeys_13 = new ArrayList<String>(bulkOfKeys_13_size);
      this.bulkOfKeys_14 = new ArrayList<String>(bulkOfKeys_14_size);
      this.bulkOfKeys_15 = new ArrayList<String>(bulkOfKeys_15_size);


      for(Long seq : this.recordToSubIndex_tbl.keySet()){
        //sel = (int)((Long.valueOf(seq).longValue()/10L)%16L);
        sel = (int)((seq.longValue()/10L)%16L);

        switch(sel){
          case 1: this.bulkOfKeys_1.add(seq.toString()); break;
          case 2: this.bulkOfKeys_2.add(seq.toString()); break;
          case 3: this.bulkOfKeys_3.add(seq.toString()); break;
          case 4: this.bulkOfKeys_4.add(seq.toString()); break;
          case 5: this.bulkOfKeys_5.add(seq.toString()); break;
          case 6: this.bulkOfKeys_6.add(seq.toString()); break;
          case 7: this.bulkOfKeys_7.add(seq.toString()); break;
          case 8: this.bulkOfKeys_8.add(seq.toString()); break;
          case 9: this.bulkOfKeys_9.add(seq.toString()); break;
          case 10: this.bulkOfKeys_10.add(seq.toString()); break;
          case 11: this.bulkOfKeys_11.add(seq.toString()); break;
          case 12: this.bulkOfKeys_12.add(seq.toString()); break;
          case 13: this.bulkOfKeys_13.add(seq.toString()); break;
          case 14: this.bulkOfKeys_14.add(seq.toString()); break;
          case 15: this.bulkOfKeys_15.add(seq.toString()); break;
          default: this.bulkOfKeys_0.add(seq.toString()); break;
        }
      }
 
      Collections.shuffle(this.scramble_order);

      long temp_startT;
      long temp_endT;
               
      Response<List<String>> pipe_values_0 = null;
      Response<List<String>> pipe_values_1 = null;
      Response<List<String>> pipe_values_2 = null;
      Response<List<String>> pipe_values_3 = null;
      Response<List<String>> pipe_values_4 = null;
      Response<List<String>> pipe_values_5 = null;
      Response<List<String>> pipe_values_6 = null;
      Response<List<String>> pipe_values_7 = null;
      Response<List<String>> pipe_values_8 = null;
      Response<List<String>> pipe_values_9 = null;
      Response<List<String>> pipe_values_10 = null;
      Response<List<String>> pipe_values_11 = null;
      Response<List<String>> pipe_values_12 = null;
      Response<List<String>> pipe_values_13 = null;
      Response<List<String>> pipe_values_14 = null;
      Response<List<String>> pipe_values_15 = null;

      temp_startT = System.currentTimeMillis();
      //sLogger.info("hhwu DEBUG: reduce group BEGIN:");
      for(Integer item : this.scramble_order){
        switch(item.intValue()){
          case 1:  pipe_values_1 = this.pipeline_1.mget(this.bulkOfKeys_1.toArray(new String[0]));
                   break;
          case 2:  pipe_values_2 = this.pipeline_2.mget(this.bulkOfKeys_2.toArray(new String[0]));
                   break; 
          case 3:  pipe_values_3 = this.pipeline_3.mget(this.bulkOfKeys_3.toArray(new String[0]));
                   break;
          case 4:  pipe_values_4 = this.pipeline_4.mget(this.bulkOfKeys_4.toArray(new String[0]));
                   break;
          case 5:  pipe_values_5 = this.pipeline_5.mget(this.bulkOfKeys_5.toArray(new String[0]));
                   break;
          case 6:  pipe_values_6 = this.pipeline_6.mget(this.bulkOfKeys_6.toArray(new String[0]));
                   break;
          case 7:  pipe_values_7 = this.pipeline_7.mget(this.bulkOfKeys_7.toArray(new String[0]));
                   break;
          case 8:  pipe_values_8 = this.pipeline_8.mget(this.bulkOfKeys_8.toArray(new String[0]));
                   break;
          case 9:  pipe_values_9 = this.pipeline_9.mget(this.bulkOfKeys_9.toArray(new String[0]));
                   break;
          case 10: pipe_values_10 = this.pipeline_10.mget(this.bulkOfKeys_10.toArray(new String[0]));
                   break; 
          case 11: pipe_values_11 = this.pipeline_11.mget(this.bulkOfKeys_11.toArray(new String[0]));
                   break;
          case 12: pipe_values_12 = this.pipeline_12.mget(this.bulkOfKeys_12.toArray(new String[0]));
                   break;
          case 13: pipe_values_13 = this.pipeline_13.mget(this.bulkOfKeys_13.toArray(new String[0]));
                   break;
          case 14: pipe_values_14 = this.pipeline_14.mget(this.bulkOfKeys_14.toArray(new String[0]));
                   break;
          case 15: pipe_values_15 = this.pipeline_15.mget(this.bulkOfKeys_15.toArray(new String[0]));
                   break;
          default: pipe_values_0 = this.pipeline_0.mget(this.bulkOfKeys_0.toArray(new String[0]));
                   break;
        }
        //context.progress(); // report on progress
      }

      for(Integer item : this.scramble_order){
        switch(item.intValue()){
          case 1: this.pipeline_1.sync(); break;
          case 2: this.pipeline_2.sync(); break; 
          case 3: this.pipeline_3.sync(); break;
          case 4: this.pipeline_4.sync(); break;
          case 5: this.pipeline_5.sync(); break;
          case 6: this.pipeline_6.sync(); break;
          case 7: this.pipeline_7.sync(); break;
          case 8: this.pipeline_8.sync(); break;
          case 9: this.pipeline_9.sync(); break;
          case 10: this.pipeline_10.sync();break; 
          case 11: this.pipeline_11.sync();break;
          case 12: this.pipeline_12.sync();break;
          case 13: this.pipeline_13.sync();break;
          case 14: this.pipeline_14.sync();break;
          case 15: this.pipeline_15.sync();break;
          default: this.pipeline_0.sync();break;
        }
      }
  
      for(Integer item : this.scramble_order){
        switch(item.intValue()){
          case 1: this.bulkOfValues_1 = pipe_values_1.get();
          case 2: this.bulkOfValues_2 = pipe_values_2.get();  
          case 3: this.bulkOfValues_3 = pipe_values_3.get(); 
          case 4: this.bulkOfValues_4 = pipe_values_4.get(); 
          case 5: this.bulkOfValues_5 = pipe_values_5.get(); 
          case 6: this.bulkOfValues_6 = pipe_values_6.get(); 
          case 7: this.bulkOfValues_7 = pipe_values_7.get(); 
          case 8: this.bulkOfValues_8 = pipe_values_8.get(); 
          case 9: this.bulkOfValues_9 = pipe_values_9.get(); 
          case 10: this.bulkOfValues_10 = pipe_values_10.get();
          case 11: this.bulkOfValues_11 = pipe_values_11.get();
          case 12: this.bulkOfValues_12 = pipe_values_12.get();
          case 13: this.bulkOfValues_13 = pipe_values_13.get();
          case 14: this.bulkOfValues_14 = pipe_values_14.get();
          case 15: this.bulkOfValues_15 = pipe_values_15.get();
          default: this.bulkOfValues_0 = pipe_values_0.get();
        }
      }

      pipe_values_0 = null;
      pipe_values_1 = null;
      pipe_values_2 = null;
      pipe_values_3 = null;
      pipe_values_4 = null;
      pipe_values_5 = null;
      pipe_values_6 = null;
      pipe_values_7 = null;
      pipe_values_8 = null;
      pipe_values_9 = null;
      pipe_values_10 = null;
      pipe_values_11 = null;
      pipe_values_12 = null;
      pipe_values_13 = null;
      pipe_values_14 = null;
      pipe_values_15 = null;

      temp_endT = System.currentTimeMillis();
      
 
      //sLogger.info("Accumulated Reduce group size: "+this.get_size+"  time: "+(temp_endT-temp_startT)+" ms");
      //sLogger.info("Speed of getting data from 16 Redises: "+0.2*this.get_size/(temp_endT-temp_startT)+" MB/sec");


      displayKeyValue(this.recordToSubIndex_tbl, context);

      this.recordToSubIndex_tbl.clear();

      this.bulkOfKeys_0.clear();
      this.bulkOfKeys_1.clear();
      this.bulkOfKeys_2.clear();
      this.bulkOfKeys_3.clear();
      this.bulkOfKeys_4.clear();
      this.bulkOfKeys_5.clear();
      this.bulkOfKeys_6.clear();
      this.bulkOfKeys_7.clear();
      this.bulkOfKeys_8.clear();
      this.bulkOfKeys_9.clear();
      this.bulkOfKeys_10.clear();
      this.bulkOfKeys_11.clear();
      this.bulkOfKeys_12.clear();
      this.bulkOfKeys_13.clear();
      this.bulkOfKeys_14.clear();
      this.bulkOfKeys_15.clear();

    }



    private String decodePrefix(long f_key, int num_prefix){
      int digit;
      StringBuilder buffer = new StringBuilder(); 

      for(int i=1;i< num_prefix;i++){
        digit = (int)(f_key % 5L);

        switch(digit){
          case 1: buffer.insert(0,"A"); break;
          case 2: buffer.insert(0,"C"); break;
          case 3: buffer.insert(0,"G"); break;
          case 4: buffer.insert(0,"T"); break;
          default: break;
        }

        f_key -= digit;
        f_key = f_key/5L;
      }

      switch((int)f_key){
        case 1: buffer.insert(0,"A"); break;
        case 2: buffer.insert(0,"C"); break;
        case 3: buffer.insert(0,"G"); break;
        case 4: buffer.insert(0,"T"); break;
        default: break;
      }
      buffer.append("$");

      return buffer.toString();

    }

    private void getSuffixFromRead(ArrayList <SeqNoSuffixOffset> sortedSuffix,  
                                   ArrayList <String> bulkOfKeys,
                                   List <String> bulkOfValues){
      String [] offset;
      String read;
      String seqNo;

      SeqNoSuffixOffset element;

      for(int j=0;j<bulkOfKeys.size();j++){
        seqNo = bulkOfKeys.get(j);
        read = bulkOfValues.get(j);

        offset = this.recordToSubIndex_tbl.get(Long.valueOf(seqNo)).toString().split("-");

        if(read == null){
          int sel = (int)((Long.valueOf(seqNo).longValue()/10L)%16L);

          switch(sel){
            case 1: read = this.pool_1.getResource().get(seqNo);break;
            case 2: read = this.pool_2.getResource().get(seqNo);break;
            case 3: read = this.pool_3.getResource().get(seqNo);break;
            case 4: read = this.pool_4.getResource().get(seqNo);break;
            case 5: read = this.pool_5.getResource().get(seqNo);break;
            case 6: read = this.pool_6.getResource().get(seqNo);break;
            case 7: read = this.pool_7.getResource().get(seqNo);break;
            case 8: read = this.pool_8.getResource().get(seqNo);break;
            case 9: read = this.pool_9.getResource().get(seqNo);break;
            case 10:read = this.pool_10.getResource().get(seqNo);break;
            case 11:read = this.pool_11.getResource().get(seqNo);break;
            case 12:read = this.pool_12.getResource().get(seqNo);break;
            case 13:read = this.pool_13.getResource().get(seqNo);break;
            case 14:read = this.pool_14.getResource().get(seqNo);break;
            case 15:read = this.pool_15.getResource().get(seqNo);break;
            default:read = this.pool_0.getResource().get(seqNo);break;
          }
          sLogger.info("hhwu DEBUG: get null read: = "+seqNo);
        }

        for(int i=0;i<offset.length;i++){
          //int idx = Integer.valueOf(offset[i]).intValue();

          element = new SeqNoSuffixOffset();
          element.seqNo = Long.valueOf(seqNo).longValue();
          element.offset = Integer.valueOf(offset[i]).intValue();

          //if(element.offset == read.length()){
          //  element.suffix = "$";
          //}
          //else{
            StringBuilder buffer = new StringBuilder(read.substring(element.offset));
            buffer.append("$");
            element.suffix = buffer.toString();
          //}

          sortedSuffix.add(element);
        }

      }
    }

    private void displayKeyValue(Map <Long, StringBuilder> recordToSubIndex_tbl,
                                 Context context) throws IOException, InterruptedException {
    
      int capacity = this.bulkOfKeys_0.size()+this.bulkOfKeys_1.size()+this.bulkOfKeys_2.size()+this.bulkOfKeys_3.size()+this.bulkOfKeys_4.size()+this.bulkOfKeys_5.size()+this.bulkOfKeys_6.size()+this.bulkOfKeys_7.size()+this.bulkOfKeys_8.size()+this.bulkOfKeys_9.size()+this.bulkOfKeys_10.size()+this.bulkOfKeys_11.size()+this.bulkOfKeys_12.size()+this.bulkOfKeys_13.size()+this.bulkOfKeys_14.size()+this.bulkOfKeys_15.size();

      ArrayList <SeqNoSuffixOffset> sortedSuffix = new ArrayList<SeqNoSuffixOffset>(capacity);
 
      long temp_startT;
      long temp_endT;

      getSuffixFromRead(sortedSuffix, this.bulkOfKeys_0, this.bulkOfValues_0);
      getSuffixFromRead(sortedSuffix, this.bulkOfKeys_1, this.bulkOfValues_1);
      getSuffixFromRead(sortedSuffix, this.bulkOfKeys_2, this.bulkOfValues_2);
      getSuffixFromRead(sortedSuffix, this.bulkOfKeys_3, this.bulkOfValues_3);
      getSuffixFromRead(sortedSuffix, this.bulkOfKeys_4, this.bulkOfValues_4);
      getSuffixFromRead(sortedSuffix, this.bulkOfKeys_5, this.bulkOfValues_5);
      getSuffixFromRead(sortedSuffix, this.bulkOfKeys_6, this.bulkOfValues_6);
      getSuffixFromRead(sortedSuffix, this.bulkOfKeys_7, this.bulkOfValues_7);
      getSuffixFromRead(sortedSuffix, this.bulkOfKeys_8, this.bulkOfValues_8);
      getSuffixFromRead(sortedSuffix, this.bulkOfKeys_9, this.bulkOfValues_9);
      getSuffixFromRead(sortedSuffix, this.bulkOfKeys_10, this.bulkOfValues_10);
      getSuffixFromRead(sortedSuffix, this.bulkOfKeys_11, this.bulkOfValues_11);
      getSuffixFromRead(sortedSuffix, this.bulkOfKeys_12, this.bulkOfValues_12);
      getSuffixFromRead(sortedSuffix, this.bulkOfKeys_13, this.bulkOfValues_13);
      getSuffixFromRead(sortedSuffix, this.bulkOfKeys_14, this.bulkOfValues_14);
      getSuffixFromRead(sortedSuffix, this.bulkOfKeys_15, this.bulkOfValues_15);


      temp_startT = System.currentTimeMillis();
      Collections.sort(sortedSuffix);       
      temp_endT = System.currentTimeMillis();
      //sLogger.info("Sorting time: "+(temp_endT-temp_startT)+" ms");
 
      for(SeqNoSuffixOffset item: sortedSuffix){
        this.seqNumber.set(item.seqNo);
        this.suffixOffset.set(item.toString());
        context.write(this.seqNumber, this.suffixOffset);
      }
		
      //force clean
      sortedSuffix.clear();

      
    }

    private boolean isLargeGrain(int encodedPrefix){
      //10 chars
      //3
      if(encodedPrefix == 356038125)
        return true;
    
      //10
      if(encodedPrefix == 559488750)
        return true;

      //21
      if(encodedPrefix == 966389375)
        return true;

      //28
      if(encodedPrefix == 1169840000)
        return true;

      return false;
    }

    private List<String> mGetRange(String[] keys, int[] starts, int[] ends, Jedis jedis) {
      assert keys.length == starts.length && starts.length == ends.length;
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < keys.length; i++) {
	String s = String.format(",'%s',%d,%d", keys[i], starts[i], ends[i]);
	builder.append(s);
      }

      String script = String.format("return redis.call('mgetrange'%s)", builder.toString());
      return (List<String>) jedis.eval(script);
    }
}
