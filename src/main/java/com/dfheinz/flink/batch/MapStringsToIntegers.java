package com.dfheinz.flink.batch;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

public class MapStringsToIntegers {

	public static void main(String[] args) throws Exception {
		
		try {
	
			// Step 1: Get Execution Environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			ParameterTool parms = ParameterTool.fromArgs(args);
			env.getConfig().setGlobalJobParameters(parms);
			
			// Step 2: Get Data
			DataSet<String> lines = env.readTextFile("input/batch/numbers.txt");
			
			// Step 3: Perform Transformations and Operations
			DataSet<Integer> positiveIntegers = lines
				.filter(new LineFilter())
				.map(new MapStringToInteger());
					
			// Step 4: Write to Sink(s)
			positiveIntegers.print();
			positiveIntegers.writeAsText("output/positive_integers.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
			
			// Step 5: Trigger Execution
			JobExecutionResult result  =  env.execute("FilterPositiveIntegers");
		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
	private static class LineFilter implements FilterFunction<String> {
		public boolean filter(String line) throws Exception {
			try {
				if (line.trim().equals("") || line.trim().startsWith("#") || line.trim().startsWith("-")) {
					return false;
				}
				Integer value = Integer.parseInt(line);
				return true;
			} catch (Exception e) {
				return false;
			}
		}
	};
	
	private static class MapStringToInteger implements MapFunction<String, Integer> {
		public Integer map(String input) throws Exception {
			try {
				Integer value = Integer.parseInt(input);
				return value;
			} catch (Exception e) {
				System.out.println("Error: " + input);
				throw e;	
			}
		}
	}
		
}
