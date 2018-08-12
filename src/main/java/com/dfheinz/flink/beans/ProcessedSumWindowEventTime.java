package com.dfheinz.flink.beans;

import java.util.ArrayList;
import java.util.List;

import com.dfheinz.flink.utils.Utils;

public class ProcessedSumWindowEventTime {

	// Data
	private long windowStart;
	private long windowEnd;
	private List<EventBean> events = new ArrayList<EventBean>();
	private long computedSum;
	
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		for (EventBean eventBean : events) {
		  String line = String.format("WindowStart=%s WindowEnd=%s Event=%s Value=%s Timestamp=%s ComputedSum=%d\n", 
				                       getTS(windowStart), getTS(windowEnd), eventBean.getLabel(), eventBean.getValue(), getTS(eventBean.getTimestamp()), computedSum);
		  buffer.append(line);
		}
		return buffer.toString();
	}	
	
//	public String toString() {
//		StringBuffer buffer = new StringBuffer();
//		buffer.append("WindowStart=" + getTS(windowStart));
//		for (EventBean eventBean : events) {
//			buffer.append("\nEvent=" + eventBean.getLabel() + " Value=" + eventBean.getValue());
//		}
//		buffer.append("\ncomputedSum=" + computedSum);
//		buffer.append("\nWindowEnd=" + getTS(windowEnd) + "\n");
//		return buffer.toString();
//	}	
	
	
	private String getTS(long timestamp) {
		return Utils.getFormattedTimestamp(timestamp);
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
	public List<EventBean> getEvents() {
		return events;
	}
	public void setEvents(List<EventBean> events) {
		this.events = events;
	}

	public long getComputedSum() {
		return computedSum;
	}
	public void setComputedSum(long computedSum) {
		this.computedSum = computedSum;
	}
	
	
	
}
