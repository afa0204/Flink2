package com.dfheinz.flink.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Utils {

	private static SimpleDateFormat dateFormatter = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
	
	
	public static void configureCheckpoint(StreamExecutionEnvironment env, String checkpointBackend) throws Exception {
		// Set Up Checkpoints
		env.enableCheckpointing(5000L);
		
		// set mode to exactly-once (this is the default)
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		
		// allow only one checkpoint to be in progress at the same time
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

		// make sure 500 ms of progress happen between checkpoints
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500L);

		// checkpoints have to complete within one minute, or are discarded
		env.getCheckpointConfig().setCheckpointTimeout(10000);

		// allow only one checkpoint to be in progress at the same time
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

		// Checkpoint Back-end
		env.setStateBackend((StateBackend)new FsStateBackend(checkpointBackend));
		// env.setStateBackend((StateBackend)new FsStateBackend("file:///home/hadoop/flink/checkpoints/"));
		// env.setStateBackend((StateBackend)new FsStateBackend("hdfs://captain:9000/flink/checkpoints"));
					
		// Enable externalized checkpoints which are retained after job cancellation
		// env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
	}
	
	public static void configureRestart(StreamExecutionEnvironment env) throws Exception {
		
		// Restart Strategy
		// Fixed Delay
		int restartAttempts = 3;
		int restartDelaySeconds = 5;
		long delayBetweenRestarts = restartDelaySeconds*1000;
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts, delayBetweenRestarts));
		
		// Failure Rate Restart
		int failureRate = 3;   
		Time failureInterval = Time.of(5, TimeUnit.MINUTES);
		Time delayInterval = Time.of(5, TimeUnit.SECONDS);
		// env.setRestartStrategy(RestartStrategies.failureRateRestart(failureRate, failureInterval, delayInterval));
		
		// No Restart
		// env.setRestartStrategy(RestartStrategies.noRestart());
	}	
	

	
	public static void configureRestartFixedDelay(StreamExecutionEnvironment env) throws Exception {
		
		// Restart Strategy
		// Fixed Delay
		int restartAttempts = 3;
		int restartDelaySeconds = 5;
		long delayBetweenRestarts = restartDelaySeconds*1000;
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts, delayBetweenRestarts));
	}
	
	public static void configureFailureRate(StreamExecutionEnvironment env) throws Exception {
		
		// Failure Rate Restart
		int failureRate = 3;   
		Time failureInterval = Time.of(5, TimeUnit.MINUTES);
		Time delayInterval = Time.of(5, TimeUnit.SECONDS);
		env.setRestartStrategy(RestartStrategies.failureRateRestart(failureRate, failureInterval, delayInterval));
		
	}
	
	public static void configureRestartNoRestart(StreamExecutionEnvironment env) throws Exception {
		
		// No Restart
		env.setRestartStrategy(RestartStrategies.noRestart());
	}	
	
	
	public static Date getDate(long timestamp) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(timestamp);
		return calendar.getTime();
	}
	
	public static String getFormattedTimestamp(String timestamp) {
		return dateFormatter.format(getDate(Long.valueOf(timestamp)));
	}
	
	
	public static String getFormattedSystemTime() {
		return dateFormatter.format(getDate(getNow()));
	}
	
	public static String getFormattedNow() {
		return dateFormatter.format(getNow());
	}
	
	private static long getNow() {
		return System.currentTimeMillis();
	}
	
	// YYYY-MM-DDTHH:MM:SSZ
	// Z represents time zone
	// Example: 2012-06-11T10:03:03
	public static String getFormattedTimestamp(long milliseconds) {
		Date date = new Date(milliseconds);
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		String formattedDate = format.format(date);
		return formattedDate;
	}
	
	// YYYY-MM-DDTHH:MM:SSZ
	// Z represents time zone
	// Example: 2012-06-11T10:03:03-04:00
	public static String getFormattedTimestampWithZone(long milliseconds) {
		Date date = new Date(milliseconds);
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		String formattedDate = format.format(date);
		
		DateFormat offsetFormat = new SimpleDateFormat("Z");
		String offset = offsetFormat.format(date);
		offset = offset.substring(0, 3) + ":" + offset.substring(3);
		formattedDate = formattedDate + offset;
		return formattedDate;
	}

}