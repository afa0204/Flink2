package com.dfheinz.flink.beans;

import java.util.ArrayList;
import java.util.List;

public class TireRobotOrder {

	// Data
	private List<EventBean> events = new ArrayList<EventBean>();
	
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		for (EventBean eventBean : events) {
		  String line = String.format("Tire For Model=%s Msg=%s Serial=%s\n", eventBean.getKey(), eventBean.getLabel(), eventBean.getValue());
		  buffer.append(line);
		}
		return buffer.toString();
	}	

	public List<EventBean> getEvents() {
		return events;
	}
	public void setEvents(List<EventBean> events) {
		this.events = events;
	}

	
}
