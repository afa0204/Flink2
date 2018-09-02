package com.dfheinz.flink.stream.basic;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class ComputeSumForStreamParameterTool {

	public static void main(String[] args) throws Exception {		

		// Execution Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		ParameterTool parms = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(parms);
		if (!parms.has("host") || !parms.has("port")) {
			System.out.println("Usage --host to specify host");
			System.out.println("Usage --port to specify port");
			System.exit(1);
			return;
		}
		
		// Get Our Raw Data Stream
		DataStream<Tuple2<String,Long>> eventStream = env
				.socketTextStream(parms.get("host"), parms.getInt("port"))
				.map(new MessageParser())
				.keyBy(0)
				.sum(1);
		eventStream.print();
		
		
		// Execute
		env.execute("ComputeSumForStream");
	}
	
	private static class MessageParser implements MapFunction<String,Tuple2<String,Long>> {
		public Tuple2<String,Long> map(String input) throws Exception {
			String key = "StreamKey";
			Long value = Long.valueOf(input);
			return new Tuple2<String,Long>(key,value);
		}
	}
	
	
}
