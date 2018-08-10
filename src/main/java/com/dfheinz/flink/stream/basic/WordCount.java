package com.dfheinz.flink.stream.basic;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {

	public static void main(String[] args) throws Exception {
		
		// Step 1: Get Execution Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parms = ParameterTool.fromArgs(args);
		String host = "localhost";
		int port = 9999;
	
		// Step 2: Get Our Stream	
		DataStream<String> stream = env.socketTextStream(host, port);
		
		// Step 3: Perform Transformations and Operations
		DataStream<Tuple2<String, Integer>> counts = stream
				.flatMap(new LineSplitter())
				.keyBy(0)
				.sum(1);

		// Step 4: Write to Sink(s)
		counts.print();

		// Step 5: Trigger Execution
		env.execute("WordCount");
	}

	private static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			String[] tokens = value.toLowerCase().split("\\s+");
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
}
