package com.dfheinz.flink.batch.rich;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

public class FilterMoviesAndAccumulateTotals0 {
	
	// Constants
	private static final String TOTAL_NUMBER_OF_TITLES = "totalNumberOfTitles";
	private static final String TOTAL_MATCHING_TITLES = "totalMatchingTitles";
	
	public static void main(String[] args) throws Exception {
		
		try {
	
			// Get Execution Environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			ParameterTool parms = ParameterTool.fromArgs(args);
			env.getConfig().setGlobalJobParameters(parms);
							
			// Output Our Parameters
			String pattern = "future";
			
			// Setup Broadcast Variable
			DataSet<String> titlePatternsBroadcast = env.fromElements(pattern);
			
			// Tuple:
			// Sample Line:
			// 1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy
			DataSet<Tuple3<Integer, String, String>> allMovies = env.readCsvFile("input/movies.csv")
					.ignoreFirstLine()
                    .types(Integer.class, String.class, String.class);
				
//			// Get Filtered Movie List
//			DataSet<Tuple3<Integer, String, String>> filteredMovies = allMovies
//					.filter(new RichTitleFilter(pattern))
//					.withBroadcastSet(titlePatternsBroadcast, "titlePatternsBroadcast");
//			
//			// Write to Sink(s)
//			filteredMovies.print();
//			filteredMovies.writeAsText("output/filterer_movies.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
//					
//			// Trigger Execution
//			JobExecutionResult result = env.execute("FilterMovies");
//			
//			// Get Accumulator Results
//			Integer totalNumberOfTitles = result.getAccumulatorResult(TOTAL_NUMBER_OF_TITLES);
//			Integer totalMatchingTitles = result.getAccumulatorResult(TOTAL_MATCHING_TITLES);
//			System.out.println("totalNumberOfTitles=" + totalNumberOfTitles);
//			System.out.println("totalMatchingTitles=" + totalMatchingTitles);
		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
	private static class RichTitleFilter extends RichFilterFunction<Tuple3<Integer,String,String>> {
		
		private IntCounter totalNumberOfTitles = new IntCounter();
		private IntCounter totalMatchingTitles = new IntCounter();
		private List<String> titlePatterns;

		public void open(Configuration parameters) throws Exception {
			getRuntimeContext().addAccumulator(TOTAL_NUMBER_OF_TITLES, totalNumberOfTitles);
			getRuntimeContext().addAccumulator(TOTAL_MATCHING_TITLES, totalMatchingTitles);
			Collection<String> titlePatternsBroadcast = getRuntimeContext().getBroadcastVariable("titlePatternsBroadcast");
			titlePatterns = new ArrayList<String>();
			for (String nextPattern : titlePatternsBroadcast) {
				titlePatterns.add(nextPattern);
			}
		}
		
		public void close(Configuration parameters) throws Exception {
		}	
		
		
		public boolean filter(Tuple3<Integer,String,String> tuple) throws Exception {
			try {
				totalNumberOfTitles.add(1);
				String myPattern = titlePatterns.get(0);
				boolean matches = tuple.f1.toLowerCase().indexOf(myPattern.toLowerCase()) != -1;
				
				// boolean matches = tuple.f1.toLowerCase().indexOf(pattern.toLowerCase()) != -1;
				if (matches) {
					totalMatchingTitles.add(1);
				}
				return matches;
			} catch (Exception e) {
				System.out.println("Filter Error: " + tuple);
				return false;
			}
		}
	}
	
	private static class MyRichMapFunction extends RichMapFunction<String,Integer> {
		@Override
		public Integer map(String value) throws Exception {
			return null;
		}
		
	}
	
	private static class TitleFilter implements FilterFunction<Tuple3<Integer,String,String>> {
		
		private String pattern = "";
		
		public TitleFilter(String pattern) {
			this.pattern = pattern;
		}
		
		public boolean filter(Tuple3<Integer,String,String> tuple) throws Exception {
			try {
				return tuple.f1.toLowerCase().indexOf(pattern.toLowerCase()) != -1;
			} catch (Exception e) {
				System.out.println("Filter Error: " + tuple);
				return false;
			}
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
