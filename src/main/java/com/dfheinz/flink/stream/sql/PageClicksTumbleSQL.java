package com.dfheinz.flink.stream.sql;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import com.dfheinz.flink.utils.Utils;


public class PageClicksTumbleSQL {
	

	public static void main(String[] args) throws Exception {		

		// Step 1: Get Execution Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(env); 
		ParameterTool parms = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(parms);
		String hostName = "localhost";
		int port = 9999;
		
		
		// Step 2: Get Our Stream
		DataStream<Tuple3<Long,String,String>> eventStream = env
				.socketTextStream(hostName, port)
				.map(new TableStreamMapper())
				.assignTimestampsAndWatermarks(new EventMessageTimestampAssigner());
		
		// Dynamic Table From Stream
		tableEnvironment.registerDataStream("pageViews", eventStream, "pageViewTime.rowtime, username, url");
		
	    // Issue Continuous Query
		String continuousQuery =
				"SELECT TUMBLE_START(pageViewTime, INTERVAL '10' SECOND) as wstart, " +
				"TUMBLE_END(pageViewTime, INTERVAL '10' SECOND) as wend, " +
				"username, COUNT(url) as viewcount FROM pageViews " +
				"GROUP BY TUMBLE(pageViewTime, INTERVAL '10' SECOND), username";
		
		// Dynamic Table from Continuous Query
		Table windowedTable = tableEnvironment.sqlQuery(continuousQuery);
		windowedTable.printSchema();	
		
		// Convert Dynamic Table to AppendStream
		Table resultTable = windowedTable
		  .select("wstart, wend, username,viewcount");
		TupleTypeInfo<Tuple4<Timestamp,Timestamp,String,Long>> tupleTypeInfo = new TupleTypeInfo<>(
				Types.SQL_TIMESTAMP,
				Types.SQL_TIMESTAMP,
				Types.STRING,
				Types.LONG);
		DataStream<Tuple4<Timestamp,Timestamp,String,Long>> resultDataStream =
		  tableEnvironment.toAppendStream(resultTable,tupleTypeInfo);
		
		
		// Write to Sink(s)
		resultDataStream.print();
		resultDataStream.writeAsText("output/pageviews_tumbleql.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);		
		

		// Step 5: Trigger Execution
		env.execute("PageViewsTumble");
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
