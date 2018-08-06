package com.dfheinz.flink.test.streams;

import com.dfheinz.flink.test.utils.Utils;

public class EventMessage {

	private long timestamp;
	private String key;
	private String label;
	private String value;
	private double eventTimeDelay;
	private double processTimeDelay;
	
	
	public String toString() {
		String line = String.format("%s %s %s %.2f %.2f",key,label,value,eventTimeDelay,processTimeDelay);
		return line;
	}
	public String toMessage() {
		// String line = String.format("%s,%s,%s,%d,%s,%s",key,label,value,timestamp,Utils.getFormattedTimestamp(timestamp),Utils.getFormattedNow());
		String line = String.format("%s,%s,%s,%d,%s",key,label,value,timestamp,Utils.getFormattedTimestamp(timestamp));
		return line;
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

	public double getEventTimeDelay() {
		return eventTimeDelay;
	}
	public void setEventTimeDelay(double eventTimeDelay) {
		this.eventTimeDelay = eventTimeDelay;
	}
	public double getProcessTimeDelay() {
		return processTimeDelay;
	}

	public void setProcessTimeDelay(double processTimeDelay) {
		this.processTimeDelay = processTimeDelay;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	
	
}
