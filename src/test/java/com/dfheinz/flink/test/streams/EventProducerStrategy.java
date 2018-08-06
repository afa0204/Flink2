package com.dfheinz.flink.test.streams;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.dfheinz.flink.test.utils.Utils;



public class EventProducerStrategy extends SocketProducerStrategy {

	private static Logger logger = Logger.getLogger(EventProducerStrategy.class);
	private ExecutorService executor;
	
	public EventProducerStrategy(String filePath) throws Exception {
		super(filePath);
		executor = Executors.newFixedThreadPool(5);
	}
	
	protected  void createMessages() throws Exception {
		Map<String,List<EventMessage>> eventBeanMap = new HashMap<>();
		BufferedReader reader = null;
		logger.info("createMessages BEGIN");
		InputStream is = getClass().getClassLoader().getResourceAsStream(getFilePath());
		if (is == null) {
			throw new Exception("File Not Found: " + getFilePath());
		}
		
		reader = new BufferedReader(new InputStreamReader(is));
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
			double processTimeDelay = Double.parseDouble(tokens[4]);
			
			EventMessage eventBean = new EventMessage();
			eventBean.setKey(key);
			eventBean.setLabel(label);
			eventBean.setValue(value);
			eventBean.setEventTimeDelay(eventTimeDelay);
			eventBean.setProcessTimeDelay(processTimeDelay);
			
			List<EventMessage> eventBeans = eventBeanMap.get(key);
			if (eventBeans == null) {
				eventBeans = new ArrayList<EventMessage>();
				eventBeanMap.put(key, eventBeans);
			}
			eventBeans.add(eventBean);
		}
		close(reader);
		
		// Send Messages
		for (Map.Entry<String, List<EventMessage>> entry : eventBeanMap.entrySet()) {
			executor.submit(new SendMessageTask(this,entry.getKey(),entry.getValue()));
		}
		
		// Wait for Executor to Finish
		executor.awaitTermination(5, TimeUnit.MINUTES);
		logger.info("createMessages END");
	}
	
	public void sendEvent(EventMessage eventBean) {
		double eventTimeDelay = eventBean.getEventTimeDelay();
		sleep(eventTimeDelay);
		eventBean.setTimestamp(getNow());
		long processTimeDelay = (long)(eventBean.getProcessTimeDelay()*1000);
		if (processTimeDelay == 0) {
			sendMessage(eventBean.toMessage());
		} else {
			EventTimerTask timerTask = new EventTimerTask(this, eventBean);
			Timer timer = new Timer("EventTimer");
			timer.schedule(timerTask, processTimeDelay);
		}
	}
	
	
	private static class SendMessageTask implements Runnable {
		private EventProducerStrategy strategy;
		private String key;
		private List<EventMessage> eventBeans;
		
		public SendMessageTask(EventProducerStrategy strategy, String key, List<EventMessage> eventBeans) {
			this.strategy = strategy;
			this.key = key;
			this.eventBeans = eventBeans;
		}
		
		@Override
		public void run() {
			System.out.println("Task Begin: " + key);
			for (EventMessage eventBean : eventBeans) {
				strategy.sendEvent(eventBean);
			}
		}
		
		private void sleep(long milliseconds) {
			try {
				TimeUnit.MILLISECONDS.sleep(milliseconds);
			} catch (Exception e) {
				System.err.println("Task Interrupted");
			}
		}
		
	}
	
	private static class EventTimerTask extends TimerTask {
		private SocketProducerStrategy strategy;
		private EventMessage eventBean;
		
		public EventTimerTask(SocketProducerStrategy strategy, EventMessage eventBean) {
			this.strategy = strategy;
			this.eventBean = eventBean;
		}

		@Override
		public void run() {
			strategy.sendMessage(eventBean.toMessage());
		}	
	}
	
//	protected  void sendMessages() throws Exception {
//		logger.info("sendMessages BEGIN");
//		// Send messages
//		for (EventBean eventBean : eventBeans) {
//			double eventTimeDelay = eventBean.getEventTimeDelay();
//			sleep(eventTimeDelay);
//			eventBean.setTimestamp(getNow());
//			long processTimeDelay = (long)(eventBean.getProcessTimeDelay()*1000);
//			if (processTimeDelay == 0) {
//				sendMessage(eventBean.toMessage());
//			} else {
//				EventTimerTask timerTask = new EventTimerTask(this, eventBean);
//				Timer timer = new Timer("EventTimer");
//				timer.schedule(timerTask, processTimeDelay);
//			}
//		}
//		logger.info("sendMessages END");
//	}
	
	

	

	

}
