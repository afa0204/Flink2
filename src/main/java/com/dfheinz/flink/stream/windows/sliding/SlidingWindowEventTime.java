package com.dfheinz.flink.stream.windows.sliding;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.dfheinz.flink.beans.EventBean;

public class SlidingWindowEventTime {

	public static void main(String[] args) throws Exception {		

		// Step 1: Get Execution Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parms = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(parms);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		TimeCharacteristic timeCharacteristic = env.getStreamTimeCharacteristic();
		String host = "localhost";
		int port = 9999;

		
		// Step 2: Get Our Stream
		DataStream<EventBean> eventStream = env
				.socketTextStream(parms.get("host"), parms.getInt("port"))
				.map(new EventBeanParser());


		// Step 3: Perform Transformations and Operations
//		DataStream<Tuple6<String,String,String,String,String,String>> sensorWindows = eventStream
//				.keyBy(1)
//				.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//				.process(new SensorProcessWindowFunction())
//				.uid("process-window");
		
		// Step 4: Write to Sink(s)
		eventStream.print();

		
		// Step 5: Trigger Execution
		env.execute("SlindingWindowEventTime");
	}
	
	private static class EventBeanParser implements MapFunction<String,EventBean> {	
		public EventBean map(String input) throws Exception {
			String[] tokens = input.split(",");
			String key = tokens[0];
			String label = tokens[1];
			String value = tokens[2];
			long timestamp = Long.parseLong(tokens[3]);

			EventBean event = new EventBean();
			event.setKey(key);
			event.setLabel(label);
			event.setValue(value);
			event.setTimestamp(timestamp);
			event.setProcessTime(System.currentTimeMillis());
			
			return event;
		}
	}
}
