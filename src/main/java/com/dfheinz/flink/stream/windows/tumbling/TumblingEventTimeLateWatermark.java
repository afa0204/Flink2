package com.dfheinz.flink.stream.windows.tumbling;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.dfheinz.flink.beans.EventBean;
import com.dfheinz.flink.beans.ProcessedSumWindowEventTime;
import com.dfheinz.flink.utils.Utils;


public class TumblingEventTimeLateWatermark {
	
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
				.assignTimestampsAndWatermarks(new MyTimestampAssigner(Time.seconds(7)));
				// .assignTimestampsAndWatermarks(new EventBeanTimestampAssigner());
		
		// Step 3: Perform Transformations and Operations
		SingleOutputStreamOperator<ProcessedSumWindowEventTime> processedWindows = eventStream
				.keyBy("key")
				.window(TumblingEventTimeWindows.of(Time.seconds(2)))
				.process(new MyProcessFunction());
		
		// Step 4: Write to Sink(s)
		processedWindows.writeAsText("output/tumbling_event_time_watermark_delay.txt",FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		
		// Step 5: Trigger Execution
		env.execute("TumblingEventTime");
		
	}
	
	


	
	// Generic Parameters: Input, Output, Key, Window
	private static class MyProcessFunction extends ProcessWindowFunction<EventBean, ProcessedSumWindowEventTime, Tuple, TimeWindow> {
		
		// Function Parameters: Key, Context, Input Elements, Output Collector
		@Override
		public void process(Tuple key,Context context,Iterable<EventBean> inputElements, Collector<ProcessedSumWindowEventTime> collector) throws Exception {		
			System.out.println("PROCESSING WINDOW");
			ProcessedSumWindowEventTime processedSumWindow = new ProcessedSumWindowEventTime();
			processedSumWindow.setWindowStart(context.window().getStart());
			processedSumWindow.setWindowEnd(context.window().getEnd());
			long computedSum = 0;
			for (EventBean nextEvent : inputElements) {
				processedSumWindow.getEvents().add(nextEvent);
				computedSum += Long.valueOf(nextEvent.getValue());
			}
			processedSumWindow.setComputedSum(computedSum);
			collector.collect(processedSumWindow);
		}
	}
	
	
	private static class MyTimestampAssigner extends BoundedOutOfOrdernessTimestampExtractor<EventBean> {
		public MyTimestampAssigner(Time maxOutOfOrderness) {
			super(maxOutOfOrderness);
		}

		@Override
		public long extractTimestamp(EventBean element) {
			return element.getTimestamp();
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
	
	private static class EventBeanTimestampAssigner implements AssignerWithPeriodicWatermarks<EventBean> {	
		private final long MAX_LATENESS=6;
		private final long WATERMARK_ADJUSTMENT = MAX_LATENESS*1000;
					
		private long currentMaxTimestamp = 0;
		private long previousWatermark = -1;
		
	    @Override
	    public long extractTimestamp(EventBean element, long previousElementTimestamp) {
	        long timestamp = element.getTimestamp();
	        // System.out.println("Extract: " + element.getLabel() + " timestamp=" + Utils.getFormattedTimestamp(timestamp));
	        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
	        return timestamp;
	    }

	    @Override
	    public Watermark getCurrentWatermark() {
	    	long watermark = currentMaxTimestamp - WATERMARK_ADJUSTMENT;
	    	if (currentMaxTimestamp != previousWatermark) {
	    		previousWatermark = currentMaxTimestamp;
	    		System.out.println("Adjusted Watermark=" + Utils.getFormattedTimestamp(watermark));
	    	}
	    	return new Watermark(watermark);
	    }
	   
	}
	
	
	
}
