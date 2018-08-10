package com.dfheinz.flink.stream.basic;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class ComputeAverageForStream {

	public static void main(String[] args) throws Exception {		

		// Execution Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parms = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(parms);
		String host = "localhost";
		int port = 9999;

		// Get Our Raw Data Stream
		DataStream<Tuple2<String,Double>> eventStream = env
				.socketTextStream(host, port)
				.map(new TotalsMapper())
				.keyBy(0)
				.reduce(new TotalsReducer())
				.map(new AverageMapper());
		eventStream.print();
		
		
		// Execute
		env.execute("ComputeAverageForStream");
	}
	
	
	private static class TotalsMapper implements MapFunction<String,Tuple3<String,Long,Double>> {
		public Tuple3<String,Long,Double> map(String input) throws Exception {
			String key = "StreamKey";
			Double value = Double.valueOf(input);
			return new Tuple3<String,Long,Double>(key, new Long(1), value);
		}
	}
	
	public static class TotalsReducer implements ReduceFunction<Tuple3<String,Long,Double>> {
		@Override
		public Tuple3<String,Long,Double> reduce(Tuple3<String, Long, Double> tuple1, Tuple3<String, Long, Double> tuple2) throws Exception {
			String key = tuple1.f0;
			Long totalElements = tuple1.f1 + tuple2.f1;
			Double totalSum = tuple1.f2 + tuple2.f2;
			return new Tuple3<>(key, totalElements, totalSum);
		}
	}
	
	private static class AverageMapper implements MapFunction<Tuple3<String,Long,Double>,Tuple2<String,Double>> {
		@Override
		public Tuple2<String,Double> map(Tuple3<String,Long,Double> input) throws Exception {
			return new Tuple2<String, Double>(input.f0,  input.f2/new Double(input.f1));
		}
	}
	
	
}
