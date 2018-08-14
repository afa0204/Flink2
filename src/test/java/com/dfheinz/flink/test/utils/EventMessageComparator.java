package com.dfheinz.flink.test.utils;

import java.util.Comparator;
import com.dfheinz.flink.test.streams.EventMessage;

public class EventMessageComparator implements Comparator<EventMessage> {

	@Override
	public int compare(EventMessage eventMessage1, EventMessage eventMessage2) {
		Long order1 = eventMessage1.getProcessOrder();
		Long order2 = eventMessage2.getProcessOrder();
		return order1.compareTo(order2);
	}

}
