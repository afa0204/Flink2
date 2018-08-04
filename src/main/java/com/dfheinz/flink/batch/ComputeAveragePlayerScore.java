package com.dfheinz.flink.batch;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

public class ComputeAveragePlayerScore {

	public static void main(String[] args) throws Exception {
		
		try {
	
			// Step 1: Get Execution Environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			ParameterTool parms = ParameterTool.fromArgs(args);
			env.getConfig().setGlobalJobParameters(parms);
			
			// Step 2: Get Data
			DataSet<Tuple2<String,Integer>> memberScores = env.fromElements(
					new Tuple2<>("TeamA", 5),
					new Tuple2<>("TeamB", 3),
					new Tuple2<>("TeamB", 7),
					new Tuple2<>("TeamA", 9),
					new Tuple2<>("TeamC", 6),
					new Tuple2<>("TeamC", 12),
					new Tuple2<>("TeamC", 6),
					new Tuple2<>("TeamB", 4),
					new Tuple2<>("TeamA", 1));
			
			// Step 3: Perform Transformations and Operations
			DataSet<Tuple2<String,Double>> averagePlayerScores = memberScores
					.map(new TeamTotalMapper())
					.groupBy(0)
					.reduce(new TeamTotalsReducer())
					.map(new AverageMapper());
					
				
			// Step 4: Write to Sink(s)
			averagePlayerScores.print();
			averagePlayerScores.writeAsText("output/average_player_score.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
			
			// Step 5: Trigger Execution
			JobExecutionResult result  =  env.execute("ComputeAveragePlayerScore");
		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
	// Team Total Mapper
	private static class TeamTotalMapper implements MapFunction<Tuple2<String,Integer>,Tuple3<String,Integer,Double>> {
		@Override
		public Tuple3<String,Integer,Double> map(Tuple2<String,Integer> input) throws Exception {
			return new Tuple3<String,Integer,Double>(input.f0, new Integer(1), new Double(input.f1));
		}
	}
	
	// TeamScoreReducer
	public static class TeamTotalsReducer implements ReduceFunction<Tuple3<String,Integer,Double>> {
		@Override
		public Tuple3<String,Integer,Double> reduce(Tuple3<String, Integer, Double> tuple1, Tuple3<String, Integer, Double> tuple2) throws Exception {
			String teamName = tuple1.f0;
			Integer totalNumberOfPlayers = tuple1.f1 + tuple2.f1;
			Double totalScoreForTeam = tuple1.f2 + tuple2.f2;
			return new Tuple3<>(teamName, totalNumberOfPlayers, totalScoreForTeam);
		}
	}
	
	private static class AverageMapper implements MapFunction<Tuple3<String,Integer,Double>,Tuple2<String,Double>> {
		@Override
		public Tuple2<String,Double> map(Tuple3<String,Integer,Double> input) throws Exception {
			return new Tuple2<String, Double>(input.f0, input.f2/input.f1);
		}
	}

		
}
