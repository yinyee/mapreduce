package edu.umd.cloud9.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

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
		// TODO Auto-generated method stub
		return 0;
	}
	
	/**
	 * MyMapper is a helper class that ...
	 */
	private static class MyMapper {
		
	}
	
	/**
	 * MyReducer is a helper class that ...
	 */
	private static class MyReducer {
		
	}
}
