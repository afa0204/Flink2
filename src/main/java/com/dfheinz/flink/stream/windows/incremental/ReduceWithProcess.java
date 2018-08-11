package com.dfheinz.flink.stream.windows.incremental;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.dfheinz.flink.beans.EventBean;
import com.dfheinz.flink.beans.ProcessedWindow;

public class ReduceWithProcess {

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

		
		// Step 3: Reduce Function
		DataStream<ProcessedWindow> processedWindows = eventStream
				.keyBy("key")
				.timeWindow(Time.seconds(5))
				.reduce(new MyReduceFunction(), new MyProcessFunction());
	
		// Step 4: Write to Sink(s)
		processedWindows.print();
		processedWindows.writeAsText("output/window_template.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		
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
	
    public static class MyReduceFunction implements ReduceFunction<EventBean> {
    	@Override
        public EventBean reduce(EventBean eventBean1, EventBean eventBean2) {
        	EventBean result = new EventBean();
        	result.setKey(eventBean1.getKey());
        	result.setLabel(eventBean1.getLabel());
        	result.setValue(eventBean1.getValue());
        	return result;
        }
    }
    
	// InputType, collector<OutputType>, KeyType, Window
	private static class MyProcessFunction extends ProcessWindowFunction<EventBean, ProcessedWindow, Tuple, TimeWindow> {
		
		public MyProcessFunction() {
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
	
	
}
