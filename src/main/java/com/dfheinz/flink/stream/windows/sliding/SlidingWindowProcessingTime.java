package com.dfheinz.flink.stream.windows.sliding;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.dfheinz.flink.beans.EventBean;
import com.dfheinz.flink.beans.ProcessedWindow;

public class SlidingWindowProcessingTime {

	public static void main(String[] args) throws Exception {		

		// Step 1: Get Execution Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parms = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(parms);
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		String host = "localhost";
		int port = 9999;

		
		// Step 2: Get Our Stream
		DataStream<EventBean> eventStream = env
				.socketTextStream(host, port)
				.map(new EventBeanParser());

		
		// Step 3: Perform Transformations and Operations
		DataStream<ProcessedWindow> processedWindows = eventStream
				.keyBy("key")
				.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
				.process(new EventBeanProcessWindowFunction());
		
		// Step 4: Write to Sink(s)
		processedWindows.print();
		processedWindows.writeAsText("output/slidingwindow_processtime.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		
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
	
	// InputType, collector<OutputType>, KeyType, Window
	private static class EventBeanProcessWindowFunction extends ProcessWindowFunction<EventBean, ProcessedWindow, Tuple, TimeWindow> {
		
		public EventBeanProcessWindowFunction() {
		}
		
		@Override
		public void process(Tuple key,
				Context context,
				Iterable<EventBean> inputElements,
				Collector<ProcessedWindow> collector) throws Exception {		
			ProcessedWindow processedWindowBean = new ProcessedWindow();
			processedWindowBean.setWindowStart(context.window().getStart());
			processedWindowBean.setWindowEnd(context.window().getEnd());
			for (EventBean nextEvent : inputElements) {
				processedWindowBean.getEvents().add(nextEvent);
			}
			collector.collect(processedWindowBean);
		}
	}
	
	// InputType, collector<OutputType>, KeyType, Window
	private static class EventBeanProcessWindowFunctionOld extends ProcessWindowFunction<EventBean, EventBean, Tuple, TimeWindow> {
		
		@Override
		public void process(Tuple key,
				Context context,
				Iterable<EventBean> inputElements,
				Collector<EventBean> collector) throws Exception {
			
//			System.out.println("\nPROCESS WINDOW BEGIN");
//			System.out.println("WindowStart=" +  Utils.getFormattedTimestamp(context.window().getStart()));
//			System.out.println("currentProcessingTime=" + Utils.getFormattedTimestamp(context.currentProcessingTime()));
//			System.out.println("currentWatermark=" + Utils.getFormattedTimestamp(context.currentWatermark()));
			
			for (EventBean nextEvent : inputElements) {
				nextEvent.setWindowStart(context.window().getStart());
				nextEvent.setWindowEnd(context.window().getEnd());
				collector.collect(nextEvent);
			}
			// System.out.println("WindowEnd=" +  Utils.getFormattedTimestamp(context.window().getEnd()));
			// System.out.println("PROCESS WINDOW END\n");
		}
	}
	
	
	
}
