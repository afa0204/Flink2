package com.dfheinz.flink.batch;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

import com.dfheinz.flink.batch.ComputeAveragePlayerScore.TeamTotalsReducer;

public class ComputeTeamScoresWithReduce {

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
					new Tuple2<>("TeamA", 9));
			
			// Step 3: Perform Transformations and Operations
			DataSet<Tuple2<String,Integer>> teamScores = memberScores
					.groupBy(0)
					.reduce(new TeamTotalsReducer());
				
			// Step 4: Write to Sink(s)
			teamScores.print();
			teamScores.writeAsText("output/teamscores.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
			
			// Step 5: Trigger Execution
			JobExecutionResult result  =  env.execute("ComputeTeamScoresWithReduce");
		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
	// TeamScoreReducer
	public static class TeamTotalsReducer implements ReduceFunction<Tuple2<String,Integer>> {
		@Override
		public Tuple2<String,Integer> reduce(Tuple2<String, Integer> tuple1, Tuple2<String, Integer> tuple2) throws Exception {
			String teamName = tuple1.f0;
			Integer totalScoreForTeam = tuple1.f1 + tuple2.f1;
			return new Tuple2<>(teamName, totalScoreForTeam);
		}
	}
	
		
}
