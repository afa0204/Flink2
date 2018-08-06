package com.dfheinz.flink.batch;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

public class SplitSentencesIntoWords {

	public static void main(String[] args) throws Exception {
		
		try {
	
			// Step 1: Get Execution Environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			ParameterTool parms = ParameterTool.fromArgs(args);
			env.getConfig().setGlobalJobParameters(parms);
			
			// Step 2: Get Data
			DataSet<String> lines = env.readTextFile("input/batch/sentences.txt");
			
			// Step 3: Perform Transformations and Operations
			DataSet<String> words = lines.flatMap(new LineSplitter());
					
			// Step 4: Write to Sink(s)
			words.print();
			words.writeAsText("output/words.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
			
			// Step 5: Trigger Execution
			JobExecutionResult result  =  env.execute("SplitSentencesIntoWords");
		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
	private static final class LineSplitter implements FlatMapFunction<String, String> {
		@Override
		public void flatMap(String value, Collector<String> out) {
			String[] tokens = value.toLowerCase().split("\\s+");
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(token);
				}
			}
		}
	}
	

		
}
