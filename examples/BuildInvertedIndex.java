package edu.umd.cloud9.examples;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.PairOfStrings;


public class BuildInvertedIndex extends Configured implements Tool {
	
	private static final Logger LOGGER = Logger.getLogger(BuildInvertedIndex.class);
	
	public BuildInvertedIndex() {
		// Empty default constructor
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BuildInvertedIndex(), args);
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

		LOGGER.info("Tool: BuildInvertedIndex");
		LOGGER.info(" - input path: " + inputPath);
		LOGGER.info(" - output path: " + outputPath);
		LOGGER.info(" - number of mappers: " + numberOfMapTasks);
		LOGGER.info(" - number of reducers: " + numberOfReduceTasks);

		JobConf conf = new JobConf(BuildInvertedIndex.class);
		conf.setJobName("BuildInvertedIndex");

		conf.setNumMapTasks(numberOfMapTasks);
		conf.setNumReduceTasks(numberOfReduceTasks);

		conf.setInputFormat(TextInputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		/**
		 *  Note that these must match the Class arguments given in MyMapper 
		 */
		conf.setOutputKeyClass(PairOfStrings.class);
		conf.setOutputValueClass(LongWritable.class);

		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(MyCombiner.class);
		conf.setReducerClass(MyCombiner.class);

		conf.setLong("mapred.task.timeout", 1200000);
		
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
	 * 		key <- line number aka docno
	 * 		value <- line text
	 * We need to split each line into tokens with count of one.  Hence, the output should be:
	 * 		key <- word
	 * 		value <- (docno, 1)
	 */
	private static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, PairOfStrings, LongWritable> {

		private static final LongWritable ONE = new LongWritable(1);
		private String word = new String();
		private String docno = new String();
		private PairOfStrings wordDocno = new PairOfStrings();
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<PairOfStrings, LongWritable> output, Reporter reporter) throws IOException {
			
			docno = Long.toString(key.get());
			String line = ((Text) value).toString();
			StringTokenizer tokeniser = new StringTokenizer(line);
			
			while (tokeniser.hasMoreTokens()) {
				word = tokeniser.nextToken();
				wordDocno.set(word, docno);
				output.collect(wordDocno, ONE);
			}
		}
		
	}
	
	/**
	 * MyCombiner is a helper class that reads data which is formatted as follows:
	 * 		key <- word
	 * 		value <- (docno, 1)
	 * We need to add up the counts for each word in each document.  Hence, the output should be:
	 * 		key <- word
	 *		value <- (docno, count)
	 */
	private static class MyCombiner extends MapReduceBase implements Reducer<PairOfStrings, LongWritable, PairOfStrings, LongWritable> {

		private static LongWritable SUM = new LongWritable();
		
		@Override
		public void reduce(PairOfStrings key, Iterator<LongWritable> values, OutputCollector<PairOfStrings, LongWritable> output, Reporter reporter) throws IOException {
			long sum = 0;
			while (values.hasNext()) {
				sum += 1;
			}
			SUM.set(sum);
			output.collect(key, SUM);
		}
		
	}
	
	
	/**
	 * MyReducer is a helper class that reads data which is formatted as follows:
	 * 		key <- word
	 * 		value <- (docno, count)
	 * We need to add up the document counts.  Hence, the output should be:
	 * 		key <- word
	 * 		value <- (doc count, (docno, count))
	 */
//	private static class MyReducer extends MapReduceBase implements Reducer<Text, PairOfLongs, Text, PairOfWritables<LongWritable, ArrayListWritable<PairOfLongs>>> {
//		
//		private ArrayListWritable<PairOfLongs> pairs = new ArrayListWritable<PairOfLongs>();
//		private PairOfLongs pair = new PairOfLongs();
//		private PairOfWritables<LongWritable, ArrayListWritable<PairOfLongs>> tfidf = new PairOfWritables<LongWritable, ArrayListWritable<PairOfLongs>>();
//		private LongWritable docCount = new LongWritable();
//		
//		@Override
//		public void reduce(Text word, Iterator<PairOfLongs> docnoCount, OutputCollector<Text, PairOfWritables<LongWritable, ArrayListWritable<PairOfLongs>>> output, Reporter reporter) throws IOException {
//			int sum = 0;
//			while (docnoCount.hasNext()) {
//				sum += 1;
//				pair = docnoCount.next().clone();
//				pairs.add(pair);
//			}
//			docCount.set(sum);
//			tfidf.set(docCount, pairs);
//			output.collect(word, tfidf);
//		}
//		
//	}

//	private static class MyReducer extends MapReduceBase implements Reducer <Text, PairOfLongs, Text, PairOfLongs>{
//
//		@Override
//		public void reduce(Text word, Iterator<PairOfLongs> docnoCount, OutputCollector<Text, PairOfLongs> output, Reporter reporter) throws IOException {
//			
//			
//			
//			while (docnoCount.hasNext()) {
//				
//			}
//			
//			
//		}
//		
//		
//		
//	}
	
	
	
}
