package com.dfheinz.flink.batch;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

public class ComputeTeamScores {

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
			DataSet<Tuple2<String,Integer>> teamScores = memberScores
					.groupBy(0)
					.sum(1);
				
			// Step 4: Write to Sink(s)
			teamScores.print();
			teamScores.writeAsText("output/teamscores.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
			
			// Step 5: Trigger Execution
			JobExecutionResult result  =  env.execute("ComputeTeamScores");
		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
		
}
