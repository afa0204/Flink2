package com.dfheinz.flink.stream.windows.count;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;



public class CountByKey {

	public static void main(String[] args) throws Exception {		
	
		// Get and Set execution parameters.
		ParameterTool parms = ParameterTool.fromArgs(args);
		if (!parms.has("host") || 
			!parms.has("port")) {
			System.out.println("Usage --host to specify host");
			System.out.println("Usage --port to specify port");
			System.exit(1);
			return;
		}		
		
		// Set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(parms);
		
		// Get Our Data Stream 
		String host1 = parms.get("host");
		int port1 = parms.getInt("port");
		DataStream<String> dataStream = env.socketTextStream(host1,port1);
		
		DataStream<Donut> donutStream = dataStream
			.map(new InputParser())
			.keyBy("type")
			.countWindow(3)
			.sum("count");
		donutStream.print();
		
//		DataStream<Donut> donutStream = dataStream
//				.map(new InputParser())
//				.keyBy("type")
//				.countWindow(3)
//				.sum("count");
//		donutStream.print();
		
		// Execute
		JobExecutionResult result  =  env.execute("CountWindow");
	}
	
	
	
	private static class ProcessDozen extends ProcessWindowFunction<Donut, Map<String,Integer>, Tuple, GlobalWindow> {
		private static final long serialVersionUID = 1L;
		public ProcessDozen() {
		}
		
		@Override
		public void process(Tuple key,
				Context context,
				Iterable<Donut> inputElements,
				Collector<Map<String,Integer>> collector) throws Exception {
			
			// collector.collect(courseMap);			
		}
	}	
	
	
	
	// Input Line
	// biology,USA
	// calculus,Germany
	// chemistry,USA
	private static class InputParser implements MapFunction<String,Donut> {
		@Override
		public Donut map(String input) throws Exception {
			String[] elements = input.split(",");
			String type = elements[0];
			return new Donut(type,1);
		}
	}
	
	public static class Donut {
		public String type;
		public Integer count;
		
		public Donut() {
		}
		
		public Donut(String type, Integer count) {
			this.type = type;
			this.count = count;
		}
		
		public String toString() {
			StringBuilder buffer = new StringBuilder();
			buffer.append("Donut.type=" + type + " count=" + count);
			return buffer.toString();
		}
		
		
	}
	
	
	
}
