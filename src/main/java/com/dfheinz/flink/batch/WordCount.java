package com.dfheinz.flink.batch;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

public class WordCount {

	public static void main(String[] args) throws Exception {
		
		try {
	
			// Step 1: Get Execution Environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			ParameterTool parms = ParameterTool.fromArgs(args);
			env.getConfig().setGlobalJobParameters(parms);
			
			// Step 2: Get Data
			DataSet<String> text = env.readTextFile(parms.get("input"));
			// DataSet<String> text = env.readTextFile("file:///Flink/input/MyData.txt");
			
			// Step 3: Perform Transformations and Operations
			DataSet<Tuple2<String, Integer>> counts = text
				.flatMap(new LineSplitter())
				.groupBy(0)
				.sum(1);
					
			// Step 4: Write to Sink(s)
			counts.print();
			// counts.writeAsText(parms.get("output"), FileSystem.WriteMode.OVERWRITE).setParallelism(1);
			counts.writeAsText("file:///Flink/output/wordcount.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
			
			// Step 5: Trigger Execution
			JobExecutionResult result  =  env.execute("WordCount");
		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
	private static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			String[] tokens = value.toLowerCase().split(" ");
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
}
