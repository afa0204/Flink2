package com.dfheinz.flink.stream.basic;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class ComputeSumByKey {

	public static void main(String[] args) throws Exception {		

		// Execution Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parms = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(parms);
		String host = "localhost";
		int port = 9999;

		// Get Our Raw Data Stream
		DataStream<Tuple2<String,Long>> eventStream = env
				.socketTextStream(host, port)
				.map(new MessageParser())
				.keyBy(0)
				.sum(1);
		eventStream.print();
		
		
		// Execute
		env.execute("ComputeSumByKey");
	}
	
	private static class MessageParser implements MapFunction<String,Tuple2<String,Long>> {
		public Tuple2<String,Long> map(String input) throws Exception {
			String[] tokens = input.toLowerCase().split(",");
			String key = tokens[0];
			Long value = Long.valueOf(tokens[1]);
			return new Tuple2<String,Long>(key,value);
		}
	}
	
	
}
