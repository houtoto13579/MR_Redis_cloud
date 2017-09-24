package sinica.iis;
import java.util.Date;
import java.io.PrintStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.examples.terasort.package-frame;

public class SuffixArrayRun{
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
          System.err.printf("Usage: SuffixArrayRun <input> <output>\n");
          System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        //Configuration conf = getConf();
        
	conf.set("fs.defaultFS", "hdfs://140.109.17.134:9000");
        conf.set("mapreduce.jobtracker.address", "140.109.17.134:54311");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.address", "140.109.17.134:8050");
	conf.set("yarn.resourcemanager.webapp.address", "140.109.17.134:8088");
	conf.set("mapreduce.jobhistory.address","140.109.17.134:10020");
	conf.set("mapreduce.jobhistory.webapp.address", "140.109.17.134:19888");
	
	long milliSeconds = 1000*60*300; //60 mins
 	conf.setLong("mapred.task.timeout", milliSeconds);
	
	conf.set("mapreduce.map.java.opts", "-Xmx2048m");
	conf.set("mapreduce.reduce.java.opts", "-Xmx7168m");	
	
	conf.set("REDIS_HOSTS", "140.109.17.134,192.168.100.102,192.168.100.105,192.168.100.106,192.168.100.110,192.168.100.111,192.168.100.115,192.168.100.116,192.168.100.117,192.168.100.118,192.168.100.119,192.168.100.120,192.168.100.121,192.168.100.122,192.168.100.123,192.168.100.124");
        //conf.set("REDIS_HOSTS", "140.109.17.134,192.168.100.102");
	conf.setInt("NUM_NODES", 16);
        conf.setInt("METHOD", 1);
	//long start = new Date().getTime();
        // time count
        long start = System.currentTimeMillis();
        
        Job job = Job.getInstance(conf);

        // Specify various job-specific parameters     
        job.setJobName("fix random mapper, 60G grouper");
        //job.setJobName("(8G reducer/7G opts)Method 1 Run SuffixArray for Bio Info (64) 160w CMS GC MGET Suffix");
        //job.setJobName("Run SuffixArray for Bio Info (32) 160W CMS AlwaysTenure NewRatio=5");
        
        job.setJarByClass(SuffixArrayRun.class);

        job.setNumReduceTasks(64);
     
        job.setMapperClass(BioMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setPartitionerClass(BioPartitioner.class);
        job.setReducerClass(BioReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);



        

        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        //MultipleInputs.addInputPath(job, new Path(args[0]), SequenceFileInputFormat.class, BmpMapper.class);
        //MultipleInputs.addInputPath(job, new Path(args[1]), SequenceFileInputFormat.class, BmpMapper.class);
        //MultipleInputs.addInputPath(job, new Path(args[2]), SequenceFileInputFormat.class, BmpMapper.class);
        //MultipleInputs.addInputPath(job, new Path(args[3]), SequenceFileInputFormat.class, BmpMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

     	// time count end
        //long end = new Date().getTime();
        //System.out.println((end - start) + "milliseconds");
        long end = System.currentTimeMillis();
        System.out.print("[TIME] =======================" + (end-start) + "==========================\r\n");  

        // Submit the job, then poll for progress until the job is complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
