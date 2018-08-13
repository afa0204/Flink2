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


public class EventReplayProducerStrategy extends SocketProducerStrategy {

	private static Logger logger = Logger.getLogger(EventReplayProducerStrategy.class);
	private List<EventMessage> eventMessages;
	
	public EventReplayProducerStrategy(String filePath) throws Exception {
		super(filePath);
	}
	
	protected  void createMessages() throws Exception {
		BufferedReader reader = null;
		logger.info("createMessages BEGIN");
		InputStream is = getClass().getClassLoader().getResourceAsStream(getFilePath());
		if (is == null) {
			throw new Exception("File Not Found: " + getFilePath());
		}
		
		reader = new BufferedReader(new InputStreamReader(is));
		String line = null;
		eventMessages = new ArrayList<>();
		while ((line=reader.readLine()) != null) {
			if (line.startsWith("#") || Utils.isBlank(line.trim())) {
				continue;
			}

			String[] tokens = line.split(",");
			String key = tokens[0];
			String label = tokens[1];
			String value = tokens[2];
			long timestamp = Long.valueOf(tokens[3]);
			double processTimeDelay = Double.parseDouble(tokens[4]);
			
			EventMessage eventMessage = new EventMessage();
			eventMessage.setKey(key);
			eventMessage.setLabel(label);
			eventMessage.setValue(value);
			eventMessage.setTimestamp(timestamp);
			// eventMessage.setEventTimeDelay(eventTimeDelay);
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
			// double eventTimeDelay = eventMessage.getEventTimeDelay();
			// sleep(eventTimeDelay);
			// eventMessage.setTimestamp(getNow());
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
