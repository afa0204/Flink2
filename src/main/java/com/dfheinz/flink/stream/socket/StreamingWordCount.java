package com.dfheinz.flink.stream.socket;


import org.apache.flink.api.common.JobExecutionResult;
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
		
		
		// Get our Parameters
		final ParameterTool parms = ParameterTool.fromArgs(args);
		if (!parms.has("host") || !parms.has("port")) {
			System.out.println("Usage: StreamingWordCount --host <host> --port <port>");
			return;
		}
		String host = parms.get("host");
		int port = parms.getInt("port");
		System.out.println("host=" + host);
		System.out.println("port=" + port);

		// Set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
	
		// Get Our Stream	
		// Split up the lines in pairs (2-tuples) containing: (word,1)
		// Group by the tuple field "0" and sum up tuple field "1"
		DataStream<Tuple2<String, Integer>> counts = env
			.socketTextStream(parms.get("host"), parms.getInt("port"))
			.flatMap(new FlatMapWordOne())
			.keyBy(0)
			.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
			.sum(1);

		counts.print();

		// Execute
		JobExecutionResult result  =  env.execute("StreamingWordCount");
	}


	// Split line into Collection of Tuple2<word, 1>
	public static final class FlatMapWordOne implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// Split the line into tokens
			String[] tokens = value.toLowerCase().split("\\W+");

			// Emit Tuple2:
			// <word, 1>
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
}
