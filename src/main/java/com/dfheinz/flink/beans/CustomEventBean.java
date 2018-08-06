package com.dfheinz.flink.beans;

import com.dfheinz.flink.utils.Utils;

public class CustomEventBean {

	private long timestamp;
	private String key;
	private String label;
	private String value;
	private double eventTimeDelay;
	private double processTimeDelay;
	
	
	public String toString() {
		String line = String.format("%s,%s,%s,%d,%s,%s",key,label,value,timestamp,Utils.getFormattedTimestamp(timestamp),Utils.getFormattedNow());
		return line;
	}
	
	public EventBean getEventBean() {
		EventBean eventBean = new EventBean();
		eventBean.setKey(key);
		eventBean.setLabel(label);
		eventBean.setValue(value);
		eventBean.setTimestamp(timestamp);
		return eventBean;
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
