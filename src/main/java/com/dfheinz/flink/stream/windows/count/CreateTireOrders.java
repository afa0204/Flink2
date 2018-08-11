package com.dfheinz.flink.stream.windows.count;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import com.dfheinz.flink.beans.EventBean;
import com.dfheinz.flink.beans.TireRobotOrder;


public class CreateTireOrders {

	public static void main(String[] args) throws Exception {
		
		// Step 1: Get Execution Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		ParameterTool parms = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(parms);
		String host = "localhost";
		int port = 9999;
		
		// Step 2: Get Our Stream
		DataStream<EventBean> dataStream = env.socketTextStream(host,port)
		 	.map(new EventBeanParser());
		
		// Transformations
		WindowedStream<EventBean,Tuple,GlobalWindow> windowedStream = dataStream
			.keyBy("key")
			.countWindow(4);
		DataStream<TireRobotOrder> robotOrders = windowedStream.apply(new CreateTireRobotOrderFunction());
		 
		// Step 4: Write to Sink(s)	 
		robotOrders.writeAsText("output/count_window_by_key.txt",FileSystem.WriteMode.OVERWRITE).setParallelism(1);
	
		// Step 5: Trigger Execution
		env.execute("CreateTireOrders");
	
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
	

	public static class CreateTireRobotOrderFunction implements WindowFunction<EventBean, TireRobotOrder, Tuple,GlobalWindow>{
		public void apply(Tuple tuple, GlobalWindow globalWindow, Iterable<EventBean> iterable, Collector<TireRobotOrder> collector) throws Exception {
			TireRobotOrder tireRobotOrder = new TireRobotOrder();
			for (EventBean eventBean: iterable) {
				tireRobotOrder.getEvents().add(eventBean);
			}
			collector.collect(tireRobotOrder);
	    }
	}

	
	
}
