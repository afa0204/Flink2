package com.dfheinz.flink.stream.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import com.dfheinz.flink.utils.Utils;

public class PageClicksCounts {
	

	public static void main(String[] args) throws Exception {		

		// Get Execution Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(env);
		String hostName = "localhost";
		int port = 9999;
		
		// Get and Set execution parameters.
		ParameterTool parms = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(parms);
		
		// Get Our Data Stream
		DataStream<Tuple3<Long,String,String>> eventStream = env
				.socketTextStream(hostName,port)
				.map(new TableStreamMapper())
				.assignTimestampsAndWatermarks(new EventMessageTimestampAssigner());
		

		// Register Table
		// tableEnvironment.registerDataStream("pageviews", eventStream);
		tableEnvironment.registerDataStream("pageViews", eventStream, "tstamp, username, url");
		
	
		// Create Table from API Query
		// Table pageViews = tableEnvironment.scan("pets");
		Table pageViews = tableEnvironment.scan("pageViews");
		pageViews.printSchema();
		
		Table countTable = pageViews
			.groupBy("username")
			.select("username, url.count as viewcount");
		
		TupleTypeInfo<Tuple2<String,Long>> tupleTypeInfo = new TupleTypeInfo<>(
				Types.STRING,
				Types.LONG);
		DataStream<Tuple2<String,Long>> resultDataStream =
		  tableEnvironment.toAppendStream(countTable,tupleTypeInfo);
		
		// Write Result Table to Sink
		// Write to Sink(s)
		resultDataStream.print();
		resultDataStream.writeAsText("output/pageviewscounts.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);		
		
		pageViews.writeToSink(new PageViewUpsertSink());
	
		// Execute
		env.execute("PageViewsCount");
	}
	
	// Sample Line:
	// Key,Label,Value,timestamp,timestampText
	// Mary,Mary.home,./home,1533991214326,08/11/2018 08:40:14
	private static class TableStreamMapper implements MapFunction<String,Tuple3<Long,String,String>> {
		public Tuple3<Long,String,String> map(String input) throws Exception {
			String[] tokens = input.toLowerCase().split(",");
			String user = tokens[0];
			long timestamp = Long.valueOf(tokens[3]);
			String url = tokens[2];	
			return new Tuple3<Long,String,String>(timestamp,user,url);
		}
	}
	
	private static class EventMessageTimestampAssigner implements AssignerWithPeriodicWatermarks<Tuple3<Long,String,String>> {
	    @Override
	    public long extractTimestamp(Tuple3<Long,String,String> element, long previousElementTimestamp) {
	        long timestamp = extractTimeStamp(element);
	        String systemTime = Utils.getFormattedSystemTime();
	        return timestamp;
	    }

	    @Override
	    public Watermark getCurrentWatermark() {
	    	long watermark = System.currentTimeMillis();
	        return new Watermark(watermark);
	    }
	    
	    private long extractTimeStamp(Tuple3<Long,String,String> element) {
	    	return element.f0;
	    }  
	}	
	
	
}
