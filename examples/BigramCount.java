package edu.umd.cloud9.examples;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * A bigram is two words that appear consecutively.
 * This class counts the number of bigrams that appear in an input file.
 * Modified from the DemoWordCount class.
 * 
 * @author yinyee
 */

public class BigramCount extends Configured implements Tool {

	private static final Logger LOGGER = Logger.getLogger(BigramCount.class);
	
	public BigramCount() {
		// Empty default constructor
	}
	
	public static int printUsage() {
		System.out.println("usage: [input-path] [output-path] [num-mappers] [num-reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// Check number of arguments provided
		if (args.length != 4) {
			printUsage();
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];

		int numberOfMapTasks = Integer.parseInt(args[2]);
		int numberOfReduceTasks = Integer.parseInt(args[3]);

		LOGGER.info("Tool: BigramCount");
		LOGGER.info(" - input path: " + inputPath);
		LOGGER.info(" - output path: " + outputPath);
		LOGGER.info(" - number of mappers: " + numberOfMapTasks);
		LOGGER.info(" - number of reducers: " + numberOfReduceTasks);

		JobConf conf = new JobConf(BigramCount.class);
		conf.setJobName("BigramCount");

		conf.setNumMapTasks(numberOfMapTasks);
		conf.setNumReduceTasks(numberOfReduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		/**
		 *  Note that these must match the Class arguments given in MyMapper 
		 */
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(MyReducer.class);
		conf.setReducerClass(MyReducer.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(outputDir.toUri(), conf).delete(outputDir, true);

		long startTime = System.currentTimeMillis();
		JobClient.runJob(conf);
		LOGGER.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BigramCount(), args);
		System.exit(res);
	}
	
	/**
	 * MyMapper is a helper class that counts the number of bigrams
	 */
	private static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		
		private static final IntWritable ONE = new IntWritable(1);
		private Text first = new Text();
		private Text second = new Text();
		private Text bigram = new Text();

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output,Reporter reporter) throws IOException {
			
			String line = ((Text) value).toString();
			StringTokenizer tokeniser = new StringTokenizer(line);
			
			if(tokeniser.hasMoreTokens()) {
				first.set(tokeniser.nextToken());
			}
			
			while(tokeniser.hasMoreTokens()) {	
				second.set(tokeniser.nextToken());
				bigram.set(first + " " + second);
				first.set(second);
				output.collect(bigram, ONE);
			}
			
		}

	}
	
	/**
	 * MyReducer is a helper class that sums up the number of unique bigrams
	 */
	private static class MyReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

		private static final IntWritable SUM = new IntWritable();
		
		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			SUM.set(sum);
			output.collect(key, SUM);
		}
	}
	
}
