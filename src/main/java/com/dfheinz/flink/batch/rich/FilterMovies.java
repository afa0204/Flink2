package com.dfheinz.flink.batch.rich;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

public class FilterMovies {
	
	public static void main(String[] args) throws Exception {
		
		try {
	
			// Step 1: Get Execution Environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			ParameterTool parms = ParameterTool.fromArgs(args);
			env.getConfig().setGlobalJobParameters(parms);
							
			// Set Our Search Parameters
			String pattern1 = "toy";
			String pattern2 = "future";
			DataSet<String> titlePatternsBroadcastVariable = env.fromElements(pattern1,pattern2);
			
			// Step 2: Get Data
			DataSet<Tuple1<String>> allTitles = env.readCsvFile("input/batch/movies.csv")
					.ignoreFirstLine()
					.includeFields("010")
					.types(String.class);
			
			// Step 3: Perform Transformations and Operations
			DataSet<Tuple1<String>> matchedTitles = allTitles
					.filter(new RichTitleFilter())
					.withBroadcastSet(titlePatternsBroadcastVariable, "titlePatternsBroadcastVariable");
			
			// Step 4: Write to Sink(s)
			matchedTitles.print();
			matchedTitles.writeAsText("output/matched_movie_titles.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
					
			// Step 5: Trigger Execution
			JobExecutionResult result = env.execute("FilterMovies");
			
		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
	private static class RichTitleFilter extends RichFilterFunction<Tuple1<String>> {
		
		private Collection<String> titlePatterns;
		
		public void open(Configuration parameters) throws Exception {
			titlePatterns = getRuntimeContext().getBroadcastVariable("titlePatternsBroadcastVariable");
		}
		
		public void close(Configuration parameters) throws Exception {
		}	
		
		public boolean filter(Tuple1<String> tuple) throws Exception {
			try {
				for (String nextPattern : titlePatterns) {
					if (tuple.f0.toLowerCase().indexOf(nextPattern) != -1) {
						return true;
					}
				}
	            return false;
			} catch (Exception e) {
				System.out.println("Filter Error: " + tuple);
				throw e;
			}
		}
	}
	
}
