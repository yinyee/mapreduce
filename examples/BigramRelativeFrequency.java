package edu.umd.cloud9.examples;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.PairOfStrings;

/**
 * This class outputs the relative frequency for selected hard-coded words in a bigram.
 * Reference: http://www.javased.com/index.php?source_dir=Cloud9/src/dist/edu/umd/cloud9/example/bigram/BigramRelativeFrequency.java
 */
public class BigramRelativeFrequency extends Configured implements Tool {

	private static final Logger LOGGER = Logger.getLogger(BigramRelativeFrequency.class);
	
	public BigramRelativeFrequency() {
		// Empty default constructor
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BigramRelativeFrequency(), args);
		System.exit(res);
	}
	
	public static int printUsage() {
		System.out.println("usage: [input-path] [output-path] [num-mappers] [num-reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 4) {
			printUsage();
			return -1;
		}
		
		String inputPath = args[0];
		String outputPath = args[1];

		int numberOfMapTasks = Integer.parseInt(args[2]);
		int numberOfReduceTasks = Integer.parseInt(args[3]);

		LOGGER.info("Tool: BigramRelativeFrequency");
		LOGGER.info(" - input path: " + inputPath);
		LOGGER.info(" - output path: " + outputPath);
		LOGGER.info(" - number of mappers: " + numberOfMapTasks);
		LOGGER.info(" - number of reducers: " + numberOfReduceTasks);

		JobConf conf = new JobConf(BigramRelativeFrequency.class);
		conf.setJobName("BigramRelativeFrequency");

		conf.setNumMapTasks(numberOfMapTasks);
		conf.setNumReduceTasks(numberOfReduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		/**
		 *  Note that these must match the Class arguments given in MyMapper 
		 */
		conf.setOutputKeyClass(PairOfStrings.class);
		conf.setOutputValueClass(FloatWritable.class);

		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(MyCombiner.class);
		conf.setReducerClass(MyReducer.class);
		conf.setPartitionerClass(MyPartitioner.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(outputDir.toUri(), conf).delete(outputDir, true);

		long startTime = System.currentTimeMillis();
		JobClient.runJob(conf);
		LOGGER.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
		
		return 0;
	}
	
	/**
	 * MyMapper is a helper class that reads data which is formatted as follows:
	 * 		first_word space second_word tab count
	 * Hence, we need to map:
	 * 		key <- first_word space second_word
	 * 		value <- count
	 * We need to add all values for keys which have the same first_word, hence we
	 * need to create a new entry:
	 * 		key <- first_word space *
	 * 		value <- sum of values for all keys with first_word
	 */
	private static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {

		private Text first = new Text();
		private PairOfStrings bigram = new PairOfStrings();
		private FloatWritable count = new FloatWritable();
		
		@Override
		public void map(LongWritable key, Text value,OutputCollector<PairOfStrings, FloatWritable> output, Reporter reporter) throws IOException {
			
			String line = ((Text) value).toString();
			StringTokenizer tokeniser = new StringTokenizer(line);
			
			while (tokeniser.hasMoreTokens()) {
				// Specific bigram
				first.set(tokeniser.nextToken());
				bigram.set(first.toString(), tokeniser.nextToken());
				count.set(Float.parseFloat(tokeniser.nextToken()));
				output.collect(bigram, count);
				
				// Generic bigram containing first_word
				bigram.set(first.toString(), "*");
				output.collect(bigram, count);
			}
		}
		
	}
	
	/**
	 * MyCombiner is a helper class that reads data which is formatted as follows:
	 * 		key <- pair of strings
	 * 		value <- count
	 * We need to add up all the values for entries with the same key, hence:
	 * 		key <- pair of strings
	 * 		values <- sum of count
	 */
	private static class MyCombiner extends MapReduceBase implements Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {

		private static final FloatWritable SUM = new FloatWritable(); 
		
		@Override
		public void reduce(PairOfStrings key, Iterator<FloatWritable> values, OutputCollector<PairOfStrings, FloatWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			SUM.set(sum);
			output.collect(key, SUM);
		}
		
	}
	
	/**
	 * MyReducer is a helper class that aggregates data which is formatted as follows:
	 * 		key <- pair of strings
	 * 		value <- count
	 * We need to add up all values for entries with the same key, hence:
	 * 		key <- pair of strings
	 * 		value <- sum of count
	 */
	private static class MyReducer extends MapReduceBase implements Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {

		private static final FloatWritable SUM = new FloatWritable();
		private float marginal;
		
		@Override
		public void reduce(PairOfStrings key, Iterator<FloatWritable> values, OutputCollector<PairOfStrings, FloatWritable> output, Reporter reporter) throws IOException {
			
			float sum = 0.0f;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			
			if (key.getRightElement().equals("*")) {
				SUM.set(sum);
				output.collect(key, SUM);
				marginal = sum;
			} else {
				SUM.set(sum / marginal);
				output.collect(key, SUM);
			}
			
		}
		
	}
	
	/**
	 * MyPartitioner is a helper class that ensures the bigrams with the same first_word get sent to the same reducer.
	 */
	private static class MyPartitioner extends MapReduceBase implements Partitioner<PairOfStrings, FloatWritable> {

		@Override
		public int getPartition(PairOfStrings key, FloatWritable value, int numberOfReducers) {
			return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numberOfReducers;
		}
		
	}
}
