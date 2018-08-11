package com.dfheinz.flink.stream.windows.tumbling;

import org.apache.flink.api.common.JobExecutionResult;
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
import org.apache.flink.util.OutputTag;

import com.dfheinz.flink.beans.EventBean;
import com.dfheinz.flink.utils.Utils;


public class TumblingEventTime {
	
	private static final OutputTag<EventBean> lateEventsTag = new OutputTag<EventBean>("late-events") {};

	public static void main(String[] args) throws Exception {		

		// Set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		TimeCharacteristic timeCharacteristic = env.getStreamTimeCharacteristic();
		env.setParallelism(1);
		
		
		String host = "localhost";
		int port = 9999;
		
		// Get and Set execution parameters.
//		ParameterTool parms = ParameterTool.fromArgs(args);
//		if (!parms.has("host") || !parms.has("port")) {
//			System.out.println("Usage --host to specify host");
//			System.out.println("Usage --port to specify port");
//			System.exit(1);
//			return;
//		}		
//		env.getConfig().setGlobalJobParameters(parms);
		

		
//		DataStream<EventBean> eventStream = env
//				.socketTextStream(host, port)
//				.map(new EventBeanParser())
//				.assignTimestampsAndWatermarks(new EventBeanTimestampAssigner());
		
		DataStream<EventBean> eventStream = env
				.socketTextStream(host, port)
				.map(new EventBeanParser())
				.assignTimestampsAndWatermarks(new MyTimestampAssigner(Time.seconds(6)));
		
		// Process Window
		SingleOutputStreamOperator<EventBean> windowedEvents = eventStream
				.keyBy("key")
				.window(TumblingEventTimeWindows.of(Time.seconds(2)))
				.sideOutputLateData(lateEventsTag)
				.process(new EventBeanProcessWindowFunction());
		windowedEvents.writeAsText("output/windowed-events.txt",FileSystem.WriteMode.OVERWRITE);
		
		
		// Late Data
		DataStream<EventBean> lateEvents = windowedEvents.getSideOutput(lateEventsTag);
		lateEvents.writeAsText("output/late-events.txt",FileSystem.WriteMode.OVERWRITE);

		// Execute
		JobExecutionResult result  =  env.execute("SensorEventTime");
	}
	

	
	// Template Parameters
	// Input
	// Output
	// Key
	// Window	
	private static class EventBeanProcessWindowFunction extends ProcessWindowFunction<EventBean, EventBean, Tuple, TimeWindow> {

		private static final long serialVersionUID = 1L;
		private String appStartTime;
		
		public EventBeanProcessWindowFunction() {
		}
		
		public EventBeanProcessWindowFunction(String appStartTime) {
			this.appStartTime = appStartTime;
		}
		

		// Parameters:
		// Key
		// Context
		// Input elements
		// Output collector
		@Override
		public void process(Tuple key,
				Context context,
				Iterable<EventBean> inputElements,
				Collector<EventBean> out) throws Exception {
			
			System.out.println("\nPROCESS WINDOW BEGIN");
			System.out.println("WindowStart=" +  Utils.getFormattedTimestamp(context.window().getStart()));
			System.out.println("currentProcessingTime=" + Utils.getFormattedTimestamp(context.currentProcessingTime()));
			System.out.println("currentWatermark=" + Utils.getFormattedTimestamp(context.currentWatermark()));
//			KeyedStateStore  windowState = context.windowState();
//			KeyedStateStore globalState = context.globalState();
			
			int count =  0;
			for (EventBean nextEvent : inputElements) {
				nextEvent.setWindowStart(context.window().getStart());
				nextEvent.setWindowEnd(context.window().getEnd());
				System.out.println(nextEvent);
				// System.out.println(nextEvent.getLabel() + " " + Utils.getFormattedTimestamp(nextEvent.getTimestamp()));
				out.collect(nextEvent);
				count++;
			}
			// System.out.println("count=" + count);
			System.out.println("WindowEnd=" +  Utils.getFormattedTimestamp(context.window().getEnd()));
			System.out.println("PROCESS WINDOW END\n");
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
	
	
	private static class EventBeanTimestampAssigner implements AssignerWithPeriodicWatermarks<EventBean> {
		
		private final long MAX_LATENESS=6;
		private final long WATERMARK_ADJUSTMENT = MAX_LATENESS*1000;
					
		private long currentMaxTimestamp = System.currentTimeMillis();
		private long previousWatermark = -1;
		
	    @Override
	    public long extractTimestamp(EventBean element, long previousElementTimestamp) {
	        long timestamp = element.getTimestamp();
	        System.out.println("Extract: " + element.getLabel() + " timestamp=" + Utils.getFormattedTimestamp(timestamp));
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
	
	private static class EventBeanParser implements MapFunction<String,EventBean> {
		
		public EventBean map(String input) throws Exception {
			return parseData(input);
		}
		
		private EventBean parseData(String input) {
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
