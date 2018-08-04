package com.dfheinz.flink.batch;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

public class FilterLines {

	public static void main(String[] args) throws Exception {
		
		try {
	
			// Step 1: Get Execution Environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			ParameterTool parms = ParameterTool.fromArgs(args);
			env.getConfig().setGlobalJobParameters(parms);
			
			// Step 2: Get Data
			DataSet<String> text = env.readTextFile("input/batch/numbers.txt");
			
			// Step 3: Perform Transformations and Operations
			DataSet<String> filteredLines = text
				.filter(new LineFilter());
					
			// Step 4: Write to Sink(s)
			filteredLines.print();
			filteredLines.writeAsText("output/filteredlines.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
			
			// Step 5: Trigger Execution
			JobExecutionResult result  =  env.execute("FilterPositiveIntegers");
		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
	private static class LineFilter implements FilterFunction<String> {
		public boolean filter(String line) throws Exception {
			try {
				if (line.trim().equals("") || line.startsWith("#") || line.startsWith("-")) {
					return false;
				} 
				Integer value = Integer.parseInt(line);
				return true;
			} catch (Exception e) {
				System.out.println("Filter Error: " + line);
				return false;
			}
		}
	}
}
