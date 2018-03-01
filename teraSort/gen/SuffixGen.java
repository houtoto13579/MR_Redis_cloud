//package testHdp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SuffixGen {

	private class CODECS {
		private static final String SNAPPY = "org.apache.hadoop.io.compress.SnappyCodec";
		private static final String LZO = "com.hadoop.compression.lzo.LzoCodec";
	}

	// compress output of mapper and reducer or not
	final static boolean compress = false;
	// number of reducer tasks
	final static int numOfReducers = 0;

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Using: " + SuffixGen.class.getName()
					+ " [input] [output]");
			return;
		}

		Configuration conf = new Configuration();

		if (compress) {
			// Hadoop 0.20 and before
			conf.setBoolean("mapred.compress.map.output", true);
			// Hadoop 0.21 and later
			conf.setBoolean("mapreduce.map.output.compress", true);

			if (conf.get("io.compression.codecs") != null) {
				if (conf.get("io.compression.codecs").contains(CODECS.SNAPPY)) {
					// Hadoop 0.20 and before
					conf.set("mapred.map.output.compression.codec",
							CODECS.SNAPPY);
					// Hadoop 0.21 and later
					conf.set("mapreduce.map.output.compress.codec",
							CODECS.SNAPPY);
				} else if (conf.get("io.compression.codecs")
						.contains(CODECS.LZO)) {
					// Hadoop 0.20 and before
					conf.set("mapred.map.output.compression.codec", CODECS.LZO);
					// Hadoop 0.21 and later
					conf.set("mapreduce.map.output.compress.codec", CODECS.LZO);
				}
			}

			// Hadoop 0.20 and before
			conf.setBoolean("mapred.output.compress", true);
			// Hadoop 0.21 and later
			conf.setBoolean("mapreduce.output.compress", true);

			if (conf.get("io.compression.codecs") != null) {
				if (conf.get("io.compression.codecs").contains(CODECS.SNAPPY)) {
					// Hadoop 0.20 and before
					conf.set("mapred.output.compression.codec", CODECS.SNAPPY);
					// Hadoop 0.21 and later
					conf.set("mapreduce.output.compression.codec",
							CODECS.SNAPPY);
				} else if (conf.get("io.compression.codecs")
						.contains(CODECS.LZO)) {
					// Hadoop 0.20 and before
					conf.set("mapred.output.compression.codec", CODECS.LZO);
					// Hadoop 0.21 and later
					conf.set("mapreduce.output.compression.codec", CODECS.LZO);
				}
			}
		}

		Job job = new Job(conf, "SuffixGen");
		job.setJarByClass(SuffixGen.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(numOfReducers);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileSystem.get(conf).delete(new Path(args[1]), true);

		job.waitForCompletion(true);
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] lineArray = value.toString().split("\t", 3);

			//StringBuilder builder = new StringBuilder(lineArray[1] + '$');
			//for (int i = 0; i < lineArray[1].length() + 1; i++) {
			StringBuilder builder = new StringBuilder(lineArray[1]);
			for (int i = 0; i < lineArray[1].length(); i++) {
				context.write(new Text(
						builder.toString() + "\t" + lineArray[0] + "," + i),
						null);

				builder.deleteCharAt(0);
			}

		}
	}

}
