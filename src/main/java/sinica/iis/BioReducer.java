package sinica.iis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
// for log4j system
import org.apache.log4j.Logger;

//redis
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


public class BioReducer extends Reducer<IntWritable, LongWritable, LongWritable, Text> {
  static final int NUM_PREFIX = 13;
  static final int TIME_OUT = 600000;
  static final int GROUP_SIZE = 1600000;
  static final int MGET_SUFFIX_SIZE = 100000;
  static final boolean NOT_SORT_YET = false;
  static final boolean START_TO_SORT = true;

  private static final Logger sLogger = Logger.getLogger(BioReducer.class.getName());

  private boolean Redis_Connection;

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

  private ArrayList <Integer> bulkOfOffsets_0;
  private ArrayList <Integer> bulkOfOffsets_1;
  private ArrayList <Integer> bulkOfOffsets_2;
  private ArrayList <Integer> bulkOfOffsets_3;
  private ArrayList <Integer> bulkOfOffsets_4;
  private ArrayList <Integer> bulkOfOffsets_5;
  private ArrayList <Integer> bulkOfOffsets_6;
  private ArrayList <Integer> bulkOfOffsets_7;
  private ArrayList <Integer> bulkOfOffsets_8;
  private ArrayList <Integer> bulkOfOffsets_9;
  private ArrayList <Integer> bulkOfOffsets_10;
  private ArrayList <Integer> bulkOfOffsets_11;
  private ArrayList <Integer> bulkOfOffsets_12;
  private ArrayList <Integer> bulkOfOffsets_13;
  private ArrayList <Integer> bulkOfOffsets_14;
  private ArrayList <Integer> bulkOfOffsets_15;

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


  //private ArrayList <Long> groupKeys;
  //private ArrayList <Integer> groupValues;

  private ArrayList <Integer> scramble_order;


  private ArrayList <SeqNoSuffixOffset> sortedSuffix;
  private int get_size;

  private LongWritable seqNumber;
  private Text suffixOffset;
 
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    
    this.jpc_0.setMaxTotal(64);
    this.jpc_1.setMaxTotal(64);
    this.jpc_2.setMaxTotal(64);
    this.jpc_3.setMaxTotal(64);
    this.jpc_4.setMaxTotal(64);
    this.jpc_5.setMaxTotal(64);
    this.jpc_6.setMaxTotal(64);
    this.jpc_7.setMaxTotal(64);
    this.jpc_8.setMaxTotal(64);
    this.jpc_9.setMaxTotal(64);
    this.jpc_10.setMaxTotal(64);
    this.jpc_11.setMaxTotal(64);
    this.jpc_12.setMaxTotal(64);
    this.jpc_13.setMaxTotal(64);
    this.jpc_14.setMaxTotal(64);
    this.jpc_15.setMaxTotal(64);


    this.pool_0  = new JedisPool(this.jpc_0, "140.109.17.134", 6379, TIME_OUT);
    this.pool_1  = new JedisPool(this.jpc_1, "192.168.100.102", 6379, TIME_OUT);
    this.pool_2  = new JedisPool(this.jpc_2, "192.168.100.112", 6379, TIME_OUT);
    this.pool_3  = new JedisPool(this.jpc_3, "192.168.100.105", 6379, TIME_OUT);
    this.pool_4  = new JedisPool(this.jpc_4, "192.168.100.106", 6379, TIME_OUT);
    this.pool_5  = new JedisPool(this.jpc_5, "192.168.100.107", 6379, TIME_OUT);
    this.pool_6  = new JedisPool(this.jpc_6, "192.168.100.118", 6379, TIME_OUT);
    this.pool_7  = new JedisPool(this.jpc_7, "192.168.100.109", 6379, TIME_OUT);
    this.pool_8  = new JedisPool(this.jpc_8, "192.168.100.110", 6379, TIME_OUT);
    this.pool_9  = new JedisPool(this.jpc_9, "192.168.100.111", 6379, TIME_OUT);
    this.pool_10 = new JedisPool(this.jpc_10, "192.168.100.119", 6379, TIME_OUT);
    this.pool_11 = new JedisPool(this.jpc_11, "192.168.100.113", 6379, TIME_OUT);
    this.pool_12 = new JedisPool(this.jpc_12, "192.168.100.121", 6379, TIME_OUT);
    this.pool_13 = new JedisPool(this.jpc_13, "192.168.100.115", 6379, TIME_OUT);
    this.pool_14 = new JedisPool(this.jpc_14, "192.168.100.116", 6379, TIME_OUT);
    this.pool_15 = new JedisPool(this.jpc_15, "192.168.100.117", 6379, TIME_OUT);



    this.get_size = 0;
    this.Redis_Connection = true;
    this.sortedSuffix = new ArrayList<SeqNoSuffixOffset>(GROUP_SIZE);
    this.seqNumber = new LongWritable();
    this.suffixOffset = new Text();

    this.bulkOfKeys_0 = new ArrayList<String>(MGET_SUFFIX_SIZE);
    this.bulkOfKeys_1 = new ArrayList<String>(MGET_SUFFIX_SIZE);
    this.bulkOfKeys_2 = new ArrayList<String>(MGET_SUFFIX_SIZE);
    this.bulkOfKeys_3 = new ArrayList<String>(MGET_SUFFIX_SIZE);
    this.bulkOfKeys_4 = new ArrayList<String>(MGET_SUFFIX_SIZE);
    this.bulkOfKeys_5 = new ArrayList<String>(MGET_SUFFIX_SIZE);
    this.bulkOfKeys_6 = new ArrayList<String>(MGET_SUFFIX_SIZE);
    this.bulkOfKeys_7 = new ArrayList<String>(MGET_SUFFIX_SIZE);
    this.bulkOfKeys_8 = new ArrayList<String>(MGET_SUFFIX_SIZE);
    this.bulkOfKeys_9 = new ArrayList<String>(MGET_SUFFIX_SIZE);
    this.bulkOfKeys_10 = new ArrayList<String>(MGET_SUFFIX_SIZE);
    this.bulkOfKeys_11 = new ArrayList<String>(MGET_SUFFIX_SIZE);
    this.bulkOfKeys_12 = new ArrayList<String>(MGET_SUFFIX_SIZE);
    this.bulkOfKeys_13 = new ArrayList<String>(MGET_SUFFIX_SIZE);
    this.bulkOfKeys_14 = new ArrayList<String>(MGET_SUFFIX_SIZE);
    this.bulkOfKeys_15 = new ArrayList<String>(MGET_SUFFIX_SIZE);

    this.bulkOfOffsets_0 = new ArrayList<Integer>(MGET_SUFFIX_SIZE);
    this.bulkOfOffsets_1 = new ArrayList<Integer>(MGET_SUFFIX_SIZE);
    this.bulkOfOffsets_2 = new ArrayList<Integer>(MGET_SUFFIX_SIZE);
    this.bulkOfOffsets_3 = new ArrayList<Integer>(MGET_SUFFIX_SIZE);
    this.bulkOfOffsets_4 = new ArrayList<Integer>(MGET_SUFFIX_SIZE);
    this.bulkOfOffsets_5 = new ArrayList<Integer>(MGET_SUFFIX_SIZE);
    this.bulkOfOffsets_6 = new ArrayList<Integer>(MGET_SUFFIX_SIZE);
    this.bulkOfOffsets_7 = new ArrayList<Integer>(MGET_SUFFIX_SIZE);
    this.bulkOfOffsets_8 = new ArrayList<Integer>(MGET_SUFFIX_SIZE);
    this.bulkOfOffsets_9 = new ArrayList<Integer>(MGET_SUFFIX_SIZE);
    this.bulkOfOffsets_10 = new ArrayList<Integer>(MGET_SUFFIX_SIZE);
    this.bulkOfOffsets_11 = new ArrayList<Integer>(MGET_SUFFIX_SIZE);
    this.bulkOfOffsets_12 = new ArrayList<Integer>(MGET_SUFFIX_SIZE);
    this.bulkOfOffsets_13 = new ArrayList<Integer>(MGET_SUFFIX_SIZE);
    this.bulkOfOffsets_14 = new ArrayList<Integer>(MGET_SUFFIX_SIZE);
    this.bulkOfOffsets_15 = new ArrayList<Integer>(MGET_SUFFIX_SIZE);


    this.scramble_order = new  ArrayList <Integer>(16);
    for(int i=0;i<16;i++)
      this.scramble_order.add(new Integer(i));

  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    int groupKeys_size = this.bulkOfKeys_0.size()+this.bulkOfKeys_1.size()+this.bulkOfKeys_2.size()+this.bulkOfKeys_3.size()
                       + this.bulkOfKeys_4.size()+this.bulkOfKeys_5.size()+this.bulkOfKeys_6.size()+this.bulkOfKeys_7.size()
                       + this.bulkOfKeys_8.size()+this.bulkOfKeys_9.size()+this.bulkOfKeys_10.size()+this.bulkOfKeys_11.size()
                       + this.bulkOfKeys_12.size()+this.bulkOfKeys_13.size()+this.bulkOfKeys_14.size()+this.bulkOfKeys_15.size();

    if(groupKeys_size > 0){
      batchProcess(context, START_TO_SORT);
      

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
      /*** initialize the jedis connections***/
      if(this.Redis_Connection){
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

        this.Redis_Connection = false;
      }

      long start = System.currentTimeMillis();
      long end;

      int sel;
      int offset;
      //String mem_key;
      Long mem_key;
      String decoded_prefix;

      int reduce_group_size = 0;
      
      if(key.get() == 0){
        StringBuilder tmp_suffix_offset = new StringBuilder("$ ");
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
      else if(isLargeGrain(key.get()) ){
        /*** process the accumulated suffixes to preserve the order ***/
        if(this.get_size > 0){
          batchProcess(context, START_TO_SORT);
          this.get_size = 0;
        }


        StringBuilder suffix_offset;

        boolean multiple_get = false;
        for(LongWritable value: values){
          offset = (int)(value.get()%1000L);
          mem_key = new Long((value.get()-offset)/1000L);
          
          dispatchKeyValuePair(mem_key, new Integer(offset));
          
          reduce_group_size++;
          this.get_size++;
 
          if(this.get_size > GROUP_SIZE){
            batchProcess(context, NOT_SORT_YET);
            this.get_size = 0;
            multiple_get = true;
          }

        }

        //outlier of reduce group
        //if(reduce_group_size > GROUP_SIZE)
        //  sLogger.info("Key "+decodePrefix(key.get(), NUM_PREFIX)+"for Reduce group size: "+reduce_group_size);

                
        if(multiple_get){
          batchProcess(context, START_TO_SORT);
          this.get_size = 0;
        }
      }
      else if(key.get()%78125 == 0){
        /*** process the accumulated suffixes to preserve the order ***/
        if(this.get_size > 0){
          batchProcess(context, START_TO_SORT);
          this.get_size = 0;
        }
       
        decoded_prefix = decodePrefix(key.get(), NUM_PREFIX);

        StringBuilder tmp_suffix_offset = new StringBuilder(decoded_prefix);;
        for(LongWritable value: values){
          offset = (int)(value.get()%1000L);
          tmp_suffix_offset.append(" ");
          tmp_suffix_offset.append(offset);

          this.seqNumber.set((value.get()-offset)/1000L);
          this.suffixOffset.set(tmp_suffix_offset.toString());
          context.write(this.seqNumber, this.suffixOffset);

          tmp_suffix_offset.delete(decoded_prefix.length(), tmp_suffix_offset.length());
          reduce_group_size++;
        }

      }
      else{
        StringBuilder suffix_offset;

        boolean multiple_get = false;
        for(LongWritable value: values){
          offset = (int)(value.get()%1000L);
          mem_key = new Long((value.get()-offset)/1000L);
       
          dispatchKeyValuePair(mem_key, new Integer(offset));
   

          reduce_group_size++;
          this.get_size++;
 
          if(this.get_size > GROUP_SIZE){
            batchProcess(context, NOT_SORT_YET);
            this.get_size = 0;
            multiple_get = true;
          }

        }


        //outlier of reduce group
        //if(reduce_group_size > GROUP_SIZE)
        //  sLogger.info("Key "+decodePrefix(key.get(), NUM_PREFIX)+"for Reduce group size: "+reduce_group_size);

                
        if(multiple_get){
          batchProcess(context, START_TO_SORT);
          this.get_size = 0;
        }
        
                

      }
      end = System.currentTimeMillis();      
      //sLogger.info("One Reduce group time("+decodePrefix(key.get(), NUM_PREFIX)+"): "+(end-start)+" ms");
      //sLogger.info("                 size: "+reduce_group_size);
        

    }//end of Reduce()


    private void batchProcess(Context context, boolean start_to_sort)throws IOException, InterruptedException{
      Collections.shuffle(this.scramble_order);

      long temp_startT;
      long temp_endT;
               
      temp_startT = System.currentTimeMillis();

      for(Integer item : this.scramble_order){
        switch(item.intValue()){
          case 1:  if(this.bulkOfKeys_1.size() != 0)
                     this.bulkOfValues_1  = mGetSuffix(this.bulkOfKeys_1,  this.bulkOfOffsets_1,   jedis1); break;
          case 2:  if(this.bulkOfKeys_2.size() != 0)
                     this.bulkOfValues_2  = mGetSuffix(this.bulkOfKeys_2,  this.bulkOfOffsets_2,   jedis2); break;
          case 3:  if(this.bulkOfKeys_3.size() != 0)
                     this.bulkOfValues_3  = mGetSuffix(this.bulkOfKeys_3,  this.bulkOfOffsets_3,   jedis3); break;
          case 4:  if(this.bulkOfKeys_4.size() != 0)
                     this.bulkOfValues_4  = mGetSuffix(this.bulkOfKeys_4,  this.bulkOfOffsets_4,   jedis4); break;
          case 5:  if(this.bulkOfKeys_5.size() != 0)
                     this.bulkOfValues_5  = mGetSuffix(this.bulkOfKeys_5,  this.bulkOfOffsets_5,   jedis5); break;
          case 6:  if(this.bulkOfKeys_6.size() != 0)
                     this.bulkOfValues_6  = mGetSuffix(this.bulkOfKeys_6,  this.bulkOfOffsets_6,   jedis6); break;
          case 7:  if(this.bulkOfKeys_7.size() != 0)
                     this.bulkOfValues_7  = mGetSuffix(this.bulkOfKeys_7,  this.bulkOfOffsets_7,   jedis7); break;
          case 8:  if(this.bulkOfKeys_8.size() != 0)
                     this.bulkOfValues_8  = mGetSuffix(this.bulkOfKeys_8,  this.bulkOfOffsets_8,   jedis8); break;
          case 9:  if(this.bulkOfKeys_9.size() != 0)
                     this.bulkOfValues_9  = mGetSuffix(this.bulkOfKeys_9,  this.bulkOfOffsets_9,   jedis9); break;
          case 10: if(this.bulkOfKeys_10.size() != 0)
                     this.bulkOfValues_10 = mGetSuffix(this.bulkOfKeys_10, this.bulkOfOffsets_10,  jedis10); break;
          case 11: if(this.bulkOfKeys_11.size() != 0)
                     this.bulkOfValues_11 = mGetSuffix(this.bulkOfKeys_11, this.bulkOfOffsets_11,  jedis11); break;
          case 12: if(this.bulkOfKeys_12.size() != 0)
                     this.bulkOfValues_12 = mGetSuffix(this.bulkOfKeys_12, this.bulkOfOffsets_12,  jedis12); break;
          case 13: if(this.bulkOfKeys_13.size() != 0)
                     this.bulkOfValues_13 = mGetSuffix(this.bulkOfKeys_13, this.bulkOfOffsets_13,  jedis13); break;
          case 14: if(this.bulkOfKeys_14.size() != 0)
                     this.bulkOfValues_14 = mGetSuffix(this.bulkOfKeys_14, this.bulkOfOffsets_14,  jedis14); break;
          case 15: if(this.bulkOfKeys_15.size() != 0)
                     this.bulkOfValues_15 = mGetSuffix(this.bulkOfKeys_15, this.bulkOfOffsets_15,  jedis15); break;
          default: if(this.bulkOfKeys_0.size() != 0)
                     this.bulkOfValues_0  = mGetSuffix(this.bulkOfKeys_0,  this.bulkOfOffsets_0,   jedis0);   break;
        }
        //context.progress(); // report on progress
      }


      temp_endT = System.currentTimeMillis();
      
 
      //sLogger.info("Accumulated Reduce group size: "+this.get_size+"  time: "+(temp_endT-temp_startT)+" ms");
      //sLogger.info("Speed of getting data from 16 Redises: "+0.2*this.get_size/(temp_endT-temp_startT)+" MB/sec");


      displayKeyValue(context, start_to_sort);

      //this.groupKeys.clear();
      //this.groupValues.clear();

      
    }

    private void dispatchKeyValuePair(Long f_key, Integer f_value){
      /*** Note that 100L means 2 characters  ***/
      int sel = (int)((f_key.longValue()/100L)%16L);

      switch(sel){
        case 1:  this.bulkOfKeys_1.add(f_key.toString());  this.bulkOfOffsets_1.add(f_value); break;
        case 2:  this.bulkOfKeys_2.add(f_key.toString());  this.bulkOfOffsets_2.add(f_value); break;
        case 3:  this.bulkOfKeys_3.add(f_key.toString());  this.bulkOfOffsets_3.add(f_value); break;
        case 4:  this.bulkOfKeys_4.add(f_key.toString());  this.bulkOfOffsets_4.add(f_value); break;
        case 5:  this.bulkOfKeys_5.add(f_key.toString());  this.bulkOfOffsets_5.add(f_value); break;
        case 6:  this.bulkOfKeys_6.add(f_key.toString());  this.bulkOfOffsets_6.add(f_value); break;
        case 7:  this.bulkOfKeys_7.add(f_key.toString());  this.bulkOfOffsets_7.add(f_value); break;
        case 8:  this.bulkOfKeys_8.add(f_key.toString());  this.bulkOfOffsets_8.add(f_value); break;
        case 9:  this.bulkOfKeys_9.add(f_key.toString());  this.bulkOfOffsets_9.add(f_value); break;
        case 10: this.bulkOfKeys_10.add(f_key.toString()); this.bulkOfOffsets_10.add(f_value); break;
        case 11: this.bulkOfKeys_11.add(f_key.toString()); this.bulkOfOffsets_11.add(f_value); break;
        case 12: this.bulkOfKeys_12.add(f_key.toString()); this.bulkOfOffsets_12.add(f_value); break;
        case 13: this.bulkOfKeys_13.add(f_key.toString()); this.bulkOfOffsets_13.add(f_value); break;
        case 14: this.bulkOfKeys_14.add(f_key.toString()); this.bulkOfOffsets_14.add(f_value); break;
        case 15: this.bulkOfKeys_15.add(f_key.toString()); this.bulkOfOffsets_15.add(f_value); break;
        default: this.bulkOfKeys_0.add(f_key.toString());  this.bulkOfOffsets_0.add(f_value); break;
      }

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

    private void prepareSuffixForSort(ArrayList <String> bulkOfKeys,
                                      List <String> bulkOfValues,
                                      ArrayList<Integer> bulkOfOffsets){
      String read;
      String seqNo;

      SeqNoSuffixOffset element;

      for(int j=0;j<bulkOfKeys.size();j++){
        element = new SeqNoSuffixOffset();
        element.seqNo = Long.valueOf(bulkOfKeys.get(j)).longValue();
        element.offset = bulkOfOffsets.get(j).intValue();

        StringBuilder buffer = new StringBuilder(bulkOfValues.get(j));
        buffer.append("$");
        element.suffix = buffer.toString();

        this.sortedSuffix.add(element);

      }
    }

    private void displayKeyValue(Context context, boolean start_to_sort) throws IOException, InterruptedException {
    
      int capacity = this.bulkOfKeys_0.size()+this.bulkOfKeys_1.size()+this.bulkOfKeys_2.size()+this.bulkOfKeys_3.size()
                   + this.bulkOfKeys_4.size()+this.bulkOfKeys_5.size()+this.bulkOfKeys_6.size()+this.bulkOfKeys_7.size()
                   + this.bulkOfKeys_8.size()+this.bulkOfKeys_9.size()+this.bulkOfKeys_10.size()+this.bulkOfKeys_11.size()
                   + this.bulkOfKeys_12.size()+this.bulkOfKeys_13.size()+this.bulkOfKeys_14.size()+this.bulkOfKeys_15.size();

      //ArrayList <SeqNoSuffixOffset> sortedSuffix = new ArrayList<SeqNoSuffixOffset>(capacity);
 
      long temp_startT;
      long temp_endT;

      
      
      prepareSuffixForSort(this.bulkOfKeys_0, this.bulkOfValues_0, this.bulkOfOffsets_0);
      this.bulkOfKeys_0.clear();
      this.bulkOfOffsets_0.clear();

      prepareSuffixForSort(this.bulkOfKeys_1, this.bulkOfValues_1, this.bulkOfOffsets_1);
      this.bulkOfKeys_1.clear();
      this.bulkOfOffsets_1.clear();

      prepareSuffixForSort(this.bulkOfKeys_2, this.bulkOfValues_2, this.bulkOfOffsets_2);
      this.bulkOfKeys_2.clear();
      this.bulkOfOffsets_2.clear();

      prepareSuffixForSort(this.bulkOfKeys_3, this.bulkOfValues_3, this.bulkOfOffsets_3);
      this.bulkOfKeys_3.clear();
      this.bulkOfOffsets_3.clear();

      prepareSuffixForSort(this.bulkOfKeys_4, this.bulkOfValues_4, this.bulkOfOffsets_4);
      this.bulkOfKeys_4.clear();
      this.bulkOfOffsets_4.clear();

      prepareSuffixForSort(this.bulkOfKeys_5, this.bulkOfValues_5, this.bulkOfOffsets_5);
      this.bulkOfKeys_5.clear();
      this.bulkOfOffsets_5.clear();

      prepareSuffixForSort(this.bulkOfKeys_6, this.bulkOfValues_6, this.bulkOfOffsets_6);
      this.bulkOfKeys_6.clear();
      this.bulkOfOffsets_6.clear();
      prepareSuffixForSort(this.bulkOfKeys_7, this.bulkOfValues_7, this.bulkOfOffsets_7);
      this.bulkOfKeys_7.clear();
      this.bulkOfOffsets_7.clear();

      prepareSuffixForSort(this.bulkOfKeys_8, this.bulkOfValues_8, this.bulkOfOffsets_8);
      this.bulkOfKeys_8.clear();
      this.bulkOfOffsets_8.clear();

      prepareSuffixForSort(this.bulkOfKeys_9, this.bulkOfValues_9, this.bulkOfOffsets_9);
      this.bulkOfKeys_9.clear();
      this.bulkOfOffsets_9.clear();

      prepareSuffixForSort(this.bulkOfKeys_10, this.bulkOfValues_10, this.bulkOfOffsets_10);
      this.bulkOfKeys_10.clear();
      this.bulkOfOffsets_10.clear();

      prepareSuffixForSort(this.bulkOfKeys_11, this.bulkOfValues_11, this.bulkOfOffsets_11);
      this.bulkOfKeys_11.clear();
      this.bulkOfOffsets_11.clear();

      prepareSuffixForSort(this.bulkOfKeys_12, this.bulkOfValues_12, this.bulkOfOffsets_12);
      this.bulkOfKeys_12.clear();
      this.bulkOfOffsets_12.clear();

      prepareSuffixForSort(this.bulkOfKeys_13, this.bulkOfValues_13, this.bulkOfOffsets_13);
      this.bulkOfKeys_13.clear();
      this.bulkOfOffsets_13.clear();

      prepareSuffixForSort(this.bulkOfKeys_14, this.bulkOfValues_14, this.bulkOfOffsets_14);
      this.bulkOfKeys_14.clear();
      this.bulkOfOffsets_14.clear();

      prepareSuffixForSort(this.bulkOfKeys_15, this.bulkOfValues_15, this.bulkOfOffsets_15);
      this.bulkOfKeys_15.clear();
      this.bulkOfOffsets_15.clear();

      if(start_to_sort){
        temp_startT = System.currentTimeMillis();
        Collections.sort(this.sortedSuffix);       
        temp_endT = System.currentTimeMillis();
        //sLogger.info("Sorting time: "+(temp_endT-temp_startT)+" ms");
 
        for(SeqNoSuffixOffset item: this.sortedSuffix){
          this.seqNumber.set(item.seqNo);
          this.suffixOffset.set(item.toString());
          context.write(this.seqNumber, this.suffixOffset);
        }
          	
        //force clean
        this.sortedSuffix.clear();
      }

      
    }

    private boolean isLargeGrain(int encodedPrefix){
      //13 chars
      if(encodedPrefix == 356038411)
        return true;
    
      if(encodedPrefix == 559488932)
        return true;

      if(encodedPrefix == 966389973)
        return true;

      if(encodedPrefix == 1169840494)
        return true;

      return false;
    }

    public static List<String> mGetSuffix(List<String> keys, List<Integer> starts, Jedis jedis) {
      assert keys.size() == starts.size();

      long [] suffix_start = new long[keys.size()];
      for(int i=0;i<keys.size();i++)
        suffix_start[i] = starts.get(i).longValue();

      return jedis.mgetsuffix(keys.toArray(new String[0]), suffix_start);
      
    }

}
