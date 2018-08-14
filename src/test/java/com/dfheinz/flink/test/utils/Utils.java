package com.dfheinz.flink.test.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Utils {
	
	private static SimpleDateFormat dateFormatter = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
	
	public static Date getDate(long timestamp) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(timestamp);
		return calendar.getTime();
	}
	
	public static long getTimestamp(String timestampValue) {
		try {
			return dateFormatter.parse(timestampValue).getTime();
		} catch (Exception e) {
			System.out.println("Unable to parse Date=" + timestampValue);
			return 0;
		}
	}
	
	public static String getFormattedTimestamp(long timestamp) {
		return dateFormatter.format(getDate(timestamp));
	}
	
	public static String getFormattedNow() {
		return dateFormatter.format(getNow());
	}
	
	public static boolean isBlank(String value) {
		return value == null || value.trim().equals("");
	}
	
	private static Date getNow() {
		Calendar calendar = Calendar.getInstance();
		return calendar.getTime();
	}

}
