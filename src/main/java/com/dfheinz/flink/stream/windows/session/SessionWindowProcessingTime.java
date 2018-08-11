package com.dfheinz.flink.stream.windows.session;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.dfheinz.flink.beans.EventBean;
import com.dfheinz.flink.beans.ProcessedSessionWindow;


public class SessionWindowProcessingTime {

	public static void main(String[] args) throws Exception {		

		// Step 1: Get Execution Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		ParameterTool parms = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(parms);
		String host = "localhost";
		int port = 9999;
		
		// Step 2: Get Our Stream
		DataStream<EventBean> eventStream = env
				.socketTextStream(host, port)
				.map(new EventBeanParser());
	
		// Step 3: Perform Transformations and Operations
		DataStream<ProcessedSessionWindow> sessionWindows = eventStream
				.keyBy("key")
				.window(ProcessingTimeSessionWindows.withGap(Time.seconds(4)))
				.process(new SessionRecordProcessWindowFunction());
		
		// Step 4: Write to Sink(s)
		sessionWindows.writeAsText("output/session_windows.txt",FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		

		// Step 5: Trigger Execution
		env.execute("SessionWindowProcessingTime");
	}
	
	// Template Parameters: Input, Output, Key, Window
	private static class SessionRecordProcessWindowFunction extends ProcessWindowFunction<EventBean, ProcessedSessionWindow, Tuple, TimeWindow> {

		// Parameters: Key, Context, Input Elements, Output Collector
		@Override
		public void process(Tuple key,Context context, Iterable<EventBean> inputElements, Collector<ProcessedSessionWindow> collector) throws Exception {
			ProcessedSessionWindow processedSessionWindow = new ProcessedSessionWindow();
			processedSessionWindow.setWindowStart(context.window().getStart());
			processedSessionWindow.setWindowEnd(context.window().getEnd());
			for (EventBean nextEvent : inputElements) {
				processedSessionWindow.getEvents().add(nextEvent);
			}
			collector.collect(processedSessionWindow);
			
		}
	}
	
	private static class EventBeanParser implements MapFunction<String,EventBean> {	
		public EventBean map(String input) throws Exception {
			String[] tokens = input.split(",");;
			EventBean event = new EventBean();
			event.setKey(tokens[0]);
			event.setLabel(tokens[1]);
			event.setValue(tokens[2]);
			event.setTimestamp(Long.parseLong(tokens[3]));
			event.setProcessTime(System.currentTimeMillis());
			return event;
		}
	}	
	


	
}
