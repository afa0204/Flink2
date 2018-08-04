package com.dfheinz.flink.stream;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class StreamingWordCount {

	public static void main(String[] args) throws Exception {
		
		// Step 1: Get Execution Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parms = ParameterTool.fromArgs(args);
		String host = parms.get("host");
		int port = parms.getInt("port");		
	
		// Step 2: Get Our Stream	
		DataStream<String> stream = env
			.socketTextStream(host, port);
		
		
		// Step 3: Perform Transformations and Operations
		// Show counts for words that arrive every 5 seconds
		DataStream<Tuple2<String, Integer>> counts = stream
				.flatMap(new LineSplitter())
				.keyBy(0)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
				.sum(1);

		// Step 4: Write to Sink(s)
		counts.print();

		// Step 5: Trigger Execution
		env.execute("StreamingWordCount");
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
