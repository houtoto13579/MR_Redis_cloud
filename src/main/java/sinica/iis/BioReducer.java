package sinica.iis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration; 

// for log4j system
import org.apache.log4j.Logger;

//redis
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;





public class BioReducer extends Reducer<IntWritable, LongWritable, LongWritable, Text> {
  static final int NUM_PREFIX = 13;
  static final int TIME_OUT = 600000;
  static final int GROUP_SIZE = 1600000;
  static final int MGET_SUFFIX_SIZE = 100000;
  static final boolean NOT_SORT_YET = false;
  static final boolean START_TO_SORT = true;

  private final Logger sLogger = Logger.getLogger(BioReducer.class.getName());

  private boolean Redis_Connection;
  
  private ArrayList<Jedis> jedisClients;
  private ArrayList<ArrayList<String>> bulksOfKeys;
  private ArrayList<ArrayList<Integer>> bulksOfOffsets;
  private ArrayList<ArrayList<String>> bulksOfValues;

  private ArrayList <Integer> scramble_order;

  private ArrayList <SeqNoSuffixOffset> sortedSuffix;
  private int get_size;

  private LongWritable seqNumber;
  private Text suffixOffset;
 
  private static int numNodes;
  private static String[] redisHosts;

  // ### testing variable init by Yueh
  private int write_counter, sequenceInReducerCounter;
  private final boolean WRITE_ALL_PARTITION = true;
  static final int COUNTTO = 100;
  private String previousKeySuffix;


  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    
    Configuration job = context.getConfiguration();
    BioReducer.numNodes = job.getInt("NUM_NODES", 1);
    BioReducer.redisHosts = job.get("REDIS_HOSTS", "localhost").split(",");
    sLogger.info("The number of node is " + numNodes);
    sLogger.info("The redis hosts are " + redisHosts[1]);
    this.bulksOfKeys = new ArrayList<>();
    this.bulksOfOffsets = new ArrayList<>();
    this.bulksOfValues = new ArrayList<>();
    this.previousKeySuffix = "TTTTTTTTTTTT";
    
    for(int i = 0; i < numNodes; i++) {
      this.bulksOfKeys.add(new ArrayList<String>(MGET_SUFFIX_SIZE));
      this.bulksOfOffsets.add(new ArrayList<Integer>(MGET_SUFFIX_SIZE));
      this.bulksOfValues.add(new ArrayList<String>());
    }

    this.get_size = 0;
    this.sortedSuffix = new ArrayList<SeqNoSuffixOffset>(GROUP_SIZE);
    this.seqNumber = new LongWritable();
    this.suffixOffset = new Text();

    this.scramble_order = new  ArrayList <Integer>(numNodes);
    for(int i=0;i<numNodes;i++)
      this.scramble_order.add(new Integer(i));

    // ### testing variable by Yueh
    this.write_counter = 0;
    this.sequenceInReducerCounter = 0;

  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    int groupKeys_size = 0;
    for(ArrayList<String> bulkOfKeys : bulksOfKeys) {
      groupKeys_size += bulkOfKeys.size();
    }
    
    if(groupKeys_size > 0){
      batchProcess(context, START_TO_SORT);
      
      for(Jedis j : jedisClients) {
          j.quit();
      }
    }
    System.out.print("&&&&&&&&&   "+ this.sequenceInReducerCounter+"   &&&&&&&&&&\r\n");

  }

    @Override
    public void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
      /*** initialize the jedis connections***/
      jedisClients = new ArrayList<>();
      for(int i = 0; i < numNodes; i++) {
        jedisClients.add(new Jedis(redisHosts[i]));
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
          
          this.write_counter += 1;
          if(partitionCounter("$")){
          	context.write(this.seqNumber, this.suffixOffset);
          }

          tmp_suffix_offset.delete(2, tmp_suffix_offset.length());
          reduce_group_size++;
        }
      }
      else if(isLargeGrain(key.get()) ){
        /*** process the accumulated suffixes to preserve the order ***/
        if(this.get_size > 0){
          batchProcess(context, START_TO_SORT);
          printSizeAndAccumulate();
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
            printSizeAndAccumulate();
            this.get_size = 0;
            multiple_get = true;
          }

        }

        //outlier of reduce group
        //if(reduce_group_size > GROUP_SIZE)
        //  sLogger.info("Key "+decodePrefix(key.get(), NUM_PREFIX)+"for Reduce group size: "+reduce_group_size);

                
        if(multiple_get){
          batchProcess(context, START_TO_SORT);
          printSizeAndAccumulate();
          this.get_size = 0;
        }
      }
      else if(key.get()%78125 == 0){
        /*** process the accumulated suffixes to preserve the order ***/
        if(this.get_size > 0){
          batchProcess(context, START_TO_SORT);
          printSizeAndAccumulate();
          this.get_size = 0;
        }
       
        decoded_prefix = decodePrefix(key.get(), NUM_PREFIX);
        
        StringBuilder tmp_suffix_offset = new StringBuilder(decoded_prefix);;
        for(LongWritable value: values){
        	//System.out.print("value: " + value.get() + "\n");   // Print value for testing
          offset = (int)(value.get()%1000L);
          tmp_suffix_offset.append(" ");
          //Print base-5 of the DNA seq for keyMapperArray in BioMapper 
          tmp_suffix_offset.append(offset);
          this.seqNumber.set((value.get()-offset)/1000L);
          this.suffixOffset.set(tmp_suffix_offset.toString());
          
          this.write_counter += 1;
          if(partitionCounter(decoded_prefix)){
          	context.write(this.seqNumber, this.suffixOffset);
          }

          tmp_suffix_offset.delete(decoded_prefix.length(), tmp_suffix_offset.length());
          reduce_group_size++;
        }

      }
      else{
        StringBuilder suffix_offset;

        boolean multiple_get = false;
        for(LongWritable value: values){
        	//System.out.print("value: " + value.get() + "\n");   // Print value for testing

          offset = (int)(value.get()%1000L);
          mem_key = new Long((value.get()-offset)/1000L);
       
          dispatchKeyValuePair(mem_key, new Integer(offset));
   

          reduce_group_size++;
          this.get_size++;
 
          if(this.get_size > GROUP_SIZE){
            batchProcess(context, NOT_SORT_YET);
            printSizeAndAccumulate();
            this.get_size = 0;
            multiple_get = true;
          }

        }


        //outlier of reduce group
        //if(reduce_group_size > GROUP_SIZE)
        //  sLogger.info("Key "+decodePrefix(key.get(), NUM_PREFIX)+"for Reduce group size: "+reduce_group_size);

                
        if(multiple_get){
          batchProcess(context, START_TO_SORT);
          printSizeAndAccumulate();
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

      for(int i : this.scramble_order){
        if (this.bulksOfKeys.get(i).size() != 0) {
          this.bulksOfValues.set(i
                      , (ArrayList<String>) mGetSuffix(this.bulksOfKeys.get(i)
                      , this.bulksOfOffsets.get(i)
                      , jedisClients.get(i))); 
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
      int sel = (int)((f_key.longValue()/100L)%numNodes);
      this.bulksOfKeys.get(sel).add(f_key.toString());
      this.bulksOfOffsets.get(sel).add(f_value);
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
        //System.out.print("ff");

        this.sortedSuffix.add(element);

      }
    }

    private void displayKeyValue(Context context, boolean start_to_sort) throws IOException, InterruptedException {
    
      int capacity = 0;
      for(ArrayList<String> bulkOfKeys : bulksOfKeys) {
        capacity += bulkOfKeys.size();
      }
      
      //ArrayList <SeqNoSuffixOffset> sortedSuffix = new ArrayList<SeqNoSuffixOffset>(capacity);
 
      long temp_startT;
      long temp_endT;

      
      for(int i = 0; i < numNodes; i++) {
        prepareSuffixForSort(this.bulksOfKeys.get(i), this.bulksOfValues.get(i), this.bulksOfOffsets.get(i));
        this.bulksOfKeys.get(i).clear();
        this.bulksOfOffsets.get(i).clear();
      }

      if(start_to_sort){
        temp_startT = System.currentTimeMillis();
        Collections.sort(this.sortedSuffix);       
        temp_endT = System.currentTimeMillis();
        //sLogger.info("Sorting time: "+(temp_endT-temp_startT)+" ms");
 
        for(SeqNoSuffixOffset item: this.sortedSuffix){
          this.seqNumber.set(item.seqNo);
          this.suffixOffset.set(item.toString());
          
          this.write_counter += 1;
          if(partitionCounter(item.suffix)){
          	context.write(this.seqNumber, this.suffixOffset);
          }
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

    private boolean partitionCounter(String suffix){
    	if (WRITE_ALL_PARTITION){
    		return true;
    	}
    	else{
          if (this.write_counter == COUNTTO){
            if(suffix!=this.previousKeySuffix){
              this.write_counter = 0;
              this.previousKeySuffix=suffix;
              return true;
            }
            this.write_counter = 0;
	  }
	  return false;
    	}
    }

    private void printSizeAndAccumulate(){
    	//System.out.print("get_size: "+ this.get_size+"\r\n");
    	this.sequenceInReducerCounter += this.get_size;
    }

}
