package com.dfheinz.flink.beans;

import com.dfheinz.flink.utils.Utils;

public class EventBean {

	// Data
	private long windowStart;
	private long windowEnd;
	private long timestamp;
	private long processTime;
	private String key;
	private String label;
	private String value;
	
	
	
	public String toString() {
		String line = String.format("%s %s %s \t%s \t%s \t%s \t%s",key,label,value,getTS(windowStart),getTS(windowEnd),getTS(timestamp),getTS(processTime));
		return line;
	}
	
	private String getTS(long timestamp) {
		if (timestamp == 0) {
			return "NULL";
		}
		return Utils.getFormattedTimestamp(timestamp);
	}
	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getWindowStart() {
		return windowStart;
	}

	public void setWindowStart(long windowStart) {
		this.windowStart = windowStart;
	}

	public long getWindowEnd() {
		return windowEnd;
	}

	public void setWindowEnd(long windowEnd) {
		this.windowEnd = windowEnd;
	}

	public long getProcessTime() {
		return processTime;
	}

	public void setProcessTime(long processTime) {
		this.processTime = processTime;
	}


	
}
