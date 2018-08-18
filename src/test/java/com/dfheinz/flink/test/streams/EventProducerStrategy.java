package com.dfheinz.flink.test.streams;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.dfheinz.flink.test.utils.Utils;


public class EventProducerStrategy extends SocketProducerStrategy {

	private static Logger logger = Logger.getLogger(EventProducerStrategy.class);
	private List<EventMessage> eventMessages;
	private String filePath;
	private int windowSize;
//	private long secondBoundary = 2;
//	private long secondBoundaryMilli = secondBoundary * 1000L;
	private long secondBoundary = 0;
	private long secondBoundaryMilli = 0;
	
	
	public EventProducerStrategy(String filePath, int windowSize) throws Exception {
		this.filePath = filePath;
		this.secondBoundary = windowSize;
		this.secondBoundaryMilli = secondBoundary * 1000L;
	}
	
	protected  void createMessages() throws Exception {
		BufferedReader reader = null;
		logger.info("createMessages BEGIN");
		InputStream is = getClass().getClassLoader().getResourceAsStream(filePath);
		if (is == null) {
			throw new Exception("File Not Found: " + filePath);
		}
		reader = new BufferedReader(new InputStreamReader(is));
		eventMessages = new ArrayList<>();
		
		// long eventTime = System.currentTimeMillis();
		long startTime = System.currentTimeMillis();
		long eventTime = (((startTime-secondBoundaryMilli)/secondBoundaryMilli)*secondBoundaryMilli) + secondBoundaryMilli;
		String line = null;
		while ((line=reader.readLine()) != null) {
			if (line.startsWith("#") || Utils.isBlank(line.trim())) {
				continue;
			}

			String[] tokens = line.split(",");
			String key = tokens[0];
			String label = tokens[1];
			String value = tokens[2];
			double eventTimeDelay = Double.parseDouble(tokens[3]);
			eventTime += (long)(eventTimeDelay*1000);
			double processTimeDelay = Double.parseDouble(tokens[4]);
			EventMessage eventMessage = new EventMessage();
			eventMessage.setKey(key);
			eventMessage.setLabel(label);
			eventMessage.setValue(value);
			eventMessage.setTimestamp(eventTime);
			eventMessage.setEventTimeDelay(eventTimeDelay);
			eventMessage.setProcessTimeDelay(processTimeDelay);
			eventMessages.add(eventMessage);
		}
		close(reader);
		
		sendMessages();
		logger.info("createMessages END");
	}
	
	private void sendMessages() throws Exception {
		logger.info("sendMessages BEGIN");
		// Send messages
		for (EventMessage eventMessage : eventMessages) {
			double eventTimeDelay = eventMessage.getEventTimeDelay();
			sleep(eventTimeDelay);
			long processTimeDelay = (long)(eventMessage.getProcessTimeDelay()*1000);
			if (processTimeDelay == 0) {
				sendMessage(eventMessage.toMessage());
			} else {
				EventTimerTask timerTask = new EventTimerTask(this, eventMessage);
				Timer timer = new Timer("EventTimer");
				timer.schedule(timerTask, processTimeDelay);
			}
		}
		logger.info("sendMessages END");
	}
	
	private static class EventTimerTask extends TimerTask {
		private SocketProducerStrategy strategy;
		private EventMessage eventMessage;
		
		public EventTimerTask(SocketProducerStrategy strategy, EventMessage eventMessage) {
			this.strategy = strategy;
			this.eventMessage = eventMessage;
		}

		@Override
		public void run() {
			strategy.sendMessage(eventMessage.toMessage());
		}
		
	}

}
