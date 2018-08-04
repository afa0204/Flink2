package com.dfheinz.flink.stream;


import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingApp {

	public static void main(String[] args) throws Exception {
		
		// Step 1: Get Execution Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parms = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(parms);
		
		// Step 2: Get Our Stream	

		// Step 3: Perform Transformations and Operations

		// Step 4: Write to Sink(s)

		// Step 5: Trigger Execution
		env.execute("MyStreamingApp");
	}

}
