package com.dfheinz.flink.stream.windows.tumbling;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.dfheinz.flink.beans.EventBean;
import com.dfheinz.flink.beans.ProcessedSumWindow;
import com.dfheinz.flink.utils.Utils;


public class TumblingEventTime {
	
	public static void main(String[] args) throws Exception {
		
		// Step 1: Get Execution Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		String host = "localhost";
		int port = 9999;
		
		// Step 2: Get Data
		DataStream<EventBean> eventStream = env
				.socketTextStream(host, port)
				.map(new EventBeanParser())
				.assignTimestampsAndWatermarks(new EventBeanTimestampAndWatermarkAssigner());
		
		// Step 3: Perform Transformations and Operations
		SingleOutputStreamOperator<ProcessedSumWindow> processedWindows = eventStream
				.keyBy("key")
				.window(TumblingEventTimeWindows.of(Time.seconds(2)))
				.process(new MyProcessFunction());
		
		// Step 4: Write to Sink(s)
		processedWindows.writeAsText("output/tumbling_event_time.txt",FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		
		// Step 5: Trigger Execution
		env.execute("TumblingEventTime");		
	}
	


	
	private static class EventBeanTimestampAndWatermarkAssigner implements AssignerWithPeriodicWatermarks<EventBean> {	
		private long currentMaxTimestamp = 0;
		
	    @Override
	    public long extractTimestamp(EventBean element, long previousElementTimestamp) {
	        long timestamp = element.getTimestamp();
	        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
	        return timestamp;
	    }

	    @Override
	    public Watermark getCurrentWatermark() {
	    	return new Watermark(currentMaxTimestamp);
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
	
	// Generic Parameters: Input, Output, Key, Window
	private static class MyProcessFunction extends ProcessWindowFunction<EventBean, ProcessedSumWindow, Tuple, TimeWindow> {
		
		// Function Parameters: Key, Context, Input Elements, Output Collector
		@Override
		public void process(Tuple key,Context context,Iterable<EventBean> inputElements, Collector<ProcessedSumWindow> collector) throws Exception {
			System.out.println("Window Output " + Utils.getFormattedTimestamp(context.window().getStart()));
			ProcessedSumWindow processedSumWindow = new ProcessedSumWindow();
			processedSumWindow.setWindowStart(context.window().getStart());
			processedSumWindow.setWindowEnd(context.window().getEnd());
			long computedSum = 0;
			for (EventBean nextEvent : inputElements) {
				System.out.println(nextEvent.getLabel() + " " + Utils.getFormattedTimestamp(nextEvent.getTimestamp()));
				processedSumWindow.getEvents().add(nextEvent);
				computedSum += Long.valueOf(nextEvent.getValue());
			}
			processedSumWindow.setComputedSum(computedSum);
			collector.collect(processedSumWindow);
			System.out.println("PROCESSING WINDOW END " + Utils.getFormattedTimestamp(context.window().getEnd()));
		}
	}
	
	
	
}
