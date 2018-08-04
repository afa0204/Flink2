package com.dfheinz.flink.batch;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

public class BatchApp {

	public static void main(String[] args) throws Exception {
		
		try {
	
			// Step 1: Get Execution Environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			ParameterTool parms = ParameterTool.fromArgs(args);
			env.getConfig().setGlobalJobParameters(parms);
			
			// Step 2: Get Data

			// Step 3: Perform Transformations and Operations
					
			// Step 4: Write to Sink(s)

			
			// Step 5: Trigger Execution
			JobExecutionResult result  =  env.execute("MyBatchApp");
		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
}
