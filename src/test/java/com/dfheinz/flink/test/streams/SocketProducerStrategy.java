package com.dfheinz.flink.test.streams;

import java.io.PrintWriter;
import java.io.Reader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;


public abstract class SocketProducerStrategy implements ProducerStrategy {
	
	private static Logger logger = Logger.getLogger(SocketProducerStrategy.class);
	private PrintWriter socketWriter;
	// private String filePath;
	
//	public SocketProducerStrategy(String filePath) throws Exception {
//		this.filePath = filePath;
//	}
	
	
	
	public void execute() throws Exception {
		createMessages();
	}
	protected abstract void createMessages() throws Exception;
	
	protected void sleep(double sleepValue) {
		try {
			if (sleepValue == 0) {
				return;
			}
			int sleepDuration = (int)(sleepValue*1000);
			Thread.sleep(sleepDuration);
		} catch (Exception e) {
			logger.error("ERROR",e);
		}
	}
	
	protected void sendMessage(String msg) {
		System.out.println(msg);
		socketWriter.println(msg);
		socketWriter.flush();
	}
	
	
	public void shutdown() {
		socketWriter.close();
	}
	
	protected long getNow() {
		return Calendar.getInstance().getTimeInMillis();
	}
	protected long getTimestamp(int delta) {
		return Calendar.getInstance().getTimeInMillis() - (delta*1000);
	}
	
	
	protected PrintWriter getSocketWriter() {
		return socketWriter;
	}


	public void setSocketWriter(PrintWriter socketWriter) {
		this.socketWriter = socketWriter;
	}
	
//	protected String getFilePath() {
//		return filePath;
//	}
	
	protected void close(Reader reader) {
		try {
			if (reader != null) {reader.close();}
		} catch (Exception e) {
		}
	}
	
	// YYYY-MM-DDTHH:MM:SSZ
	// Z represents time zone
	// Example: 2012-06-11T10:03:03
	protected String getFormattedTimestamp(long milliseconds) {
		Date date = new Date(milliseconds);
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		String formattedDate = format.format(date);
		return formattedDate;
	}
	
	// YYYY-MM-DDTHH:MM:SSZ
	// Z represents time zone
	// Example: 2012-06-11T10:03:03-04:00
	protected String getFormattedTimestampWithZone(long milliseconds) {
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
