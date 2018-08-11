package com.dfheinz.flink.stream.windows.session;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.dfheinz.flink.beans.EventBean;


public class SessionWindowEventTime {

	public static void main(String[] args) throws Exception {		

		// Set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		TimeCharacteristic timeCharacteristic = env.getStreamTimeCharacteristic();
		
		// Parallelism
		System.out.println("Parallelism=" + env.getParallelism());
		env.setParallelism(1);
		
		
		// Get and Set execution parameters.
		ParameterTool parms = ParameterTool.fromArgs(args);
		if (!parms.has("host") || !parms.has("port")) {
			System.out.println("Usage --host to specify host");
			System.out.println("Usage --port to specify port");
			System.exit(1);
			return;
		}		
		env.getConfig().setGlobalJobParameters(parms);
		
		
		// Get Our Raw Data Stream
		DataStream<EventBean> eventStream = env
				.socketTextStream(parms.get("host"), parms.getInt("port"))
				.map(new EventBeanParser())
				.assignTimestampsAndWatermarks(new MyTimestampAssigner(Time.seconds(0)));
		// eventStream.print();
		
	
		// Process Window
		DataStream<EventBean> sessionWindows = eventStream
				.keyBy("key")
				.window(EventTimeSessionWindows.withGap(Time.seconds(4)))
				.process(new SessionRecordProcessWindowFunction());
		sessionWindows.writeAsText("output/session-windows.txt",FileSystem.WriteMode.OVERWRITE);
		

		// Execute
		JobExecutionResult result  =  env.execute("SessionWindowEventTime");
	}
	
	
	
	// Template Parameters
	// Input
	// Output
	// Key
	// Window	
	private static class SessionRecordProcessWindowFunction extends ProcessWindowFunction<EventBean, EventBean, Tuple, TimeWindow> {

		private static final long serialVersionUID = 1L;
		
		public SessionRecordProcessWindowFunction() {
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
			
			// System.out.println("PROCESS WINDOW BEGIN");
		
			// System.out.println("KEY=" + key.getField(0));
			// System.out.println("WindowStart=" +  Utils.getFormattedTimestamp(context.window().getStart()));
			// System.out.println("WindowEnd=" +  context.window().getEnd() + Utils.getFormattedTimestamp(context.window().getEnd()));
			
			// System.out.println("currentProcessingTime=" + context.currentProcessingTime());
			// System.out.println("currentWatermark=" + context.currentWatermark());
			// KeyedStateStore  windowState = context.windowState();
			// KeyedStateStore globalState = context.globalState();
			
			int count =  0;
			for (EventBean nextEvent : inputElements) {
				nextEvent.setWindowStart(context.window().getStart());
				nextEvent.setWindowEnd(context.window().getEnd());
				out.collect(nextEvent);
				count++;
			}
			
			
			// System.out.println("WindowEnd=" +  Utils.getFormattedTimestamp(context.window().getEnd()));
			// System.out.println("PROCESS WINDOW END");
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
	
	
	// <timestamp,user,url,formattedTimeStamp>
	private static class SessionRecordTimestampAssigner implements AssignerWithPeriodicWatermarks<Tuple4<Long,String,String,String>> {
	    @Override
	    public long extractTimestamp(Tuple4<Long,String,String,String> element, long previousElementTimestamp) {
	    	return element.f0;
	    }

	    @Override
	    public Watermark getCurrentWatermark() {
	    	long watermark = System.currentTimeMillis();
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
