package com.dfheinz.flink.stream.basic;


import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EchoStream {

	public static void main(String[] args) throws Exception {
		
		// Step 1: Get Execution Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parms = ParameterTool.fromArgs(args);
		String host = "localhost";
		int port = 9999;
	
		// Step 2: Get Our Stream	
		DataStream<String> stream = env.socketTextStream(host, port);

		// Step 4: Write to stdout Sink
		stream.print();

		// Step 5: Trigger Execution
		env.execute("EchoStream");
	}

}
