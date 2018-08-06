package com.dfheinz.flink.stream.source;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.dfheinz.flink.beans.CustomEventBean;
import com.dfheinz.flink.beans.EventBean;

public class CustomSource implements SourceFunction<EventBean >{

	private String filePath;
	private Map<String,List<CustomEventBean>> customEventBeanMap;
	private int runInterval = 10;
	
	public CustomSource(String filePath) {
		this.filePath = filePath;
	}
	
	@Override
	public void run(SourceContext<EventBean> sourceContext) throws Exception {
		System.out.println("CustomSource.run()");
		while (true) {
			createMessages(sourceContext);
			Thread.currentThread().sleep(runInterval*60*1000);
		}
	}
	
	
	private void createMessages(SourceContext<EventBean> sourceContext) throws Exception {
		if (customEventBeanMap != null) {
			return;
		}
		BufferedReader reader = null;
		InputStream is = getClass().getClassLoader().getResourceAsStream(filePath);
		if (is == null) {
			throw new Exception("File Not Found: " + filePath);
		}
		
		reader = new BufferedReader(new InputStreamReader(is));
		String line = null;
		customEventBeanMap = new HashMap<String,List<CustomEventBean>>();
		while ((line=reader.readLine()) != null) {
			if (line.startsWith("#")) {
				continue;
			}

			// System.out.println(line);
			String[] tokens = line.split(",");
			String key = tokens[0];
			String label = tokens[1];
			String value = tokens[2];
			double eventTimeDelay = Double.parseDouble(tokens[3]);
			double processTimeDelay = Double.parseDouble(tokens[4]);
			
			CustomEventBean customEventBean = new CustomEventBean();
			customEventBean.setKey(key);
			customEventBean.setLabel(label);
			customEventBean.setValue(value);
			customEventBean.setEventTimeDelay(eventTimeDelay);
			customEventBean.setProcessTimeDelay(processTimeDelay);
			
			List<CustomEventBean> customEventBeans = customEventBeanMap.get(key);
			if (customEventBeans == null) {
				customEventBeans = new ArrayList<CustomEventBean>();
				customEventBeanMap.put(key, customEventBeans);
			}
			customEventBeans.add(customEventBean);			
		}
		close(reader);
		
		// Send Events
		for (Map.Entry<String, List<CustomEventBean>> entry : customEventBeanMap.entrySet()) {
			SendMessagesForKeyTask sendMessagesForKeyTask = new SendMessagesForKeyTask(sourceContext, entry.getKey(), entry.getValue());
			Timer timer = new Timer("SendMessagesForKey");
			timer.schedule(sendMessagesForKeyTask, 100L);
		}
		
		// Wait for Timers to Finish
		Thread.currentThread().sleep(5*60*1000);
	}
	

	
	private static class SendMessagesForKeyTask extends TimerTask {
		private SourceContext<EventBean> sourceContext;
		private String key;
		private List<CustomEventBean> customEventBeans;
		
		public SendMessagesForKeyTask(SourceContext<EventBean> sourceContext, String key, List<CustomEventBean> customEventBeans) {
			this.sourceContext = sourceContext;
			this.key = key;
			this.customEventBeans = customEventBeans;
		}
		
		@Override
		public void run() {
			System.out.println("Task Begin: " + key);
			// Set Event Times
			for (CustomEventBean customEventBean : customEventBeans) {
				long eventTimeDelay = (long)(customEventBean.getEventTimeDelay()*1000);
				sleep(eventTimeDelay);
				customEventBean.setTimestamp(System.currentTimeMillis());
				long processTimeDelay = (long)(customEventBean.getProcessTimeDelay()*1000);
				if (processTimeDelay == 0) {
					EventBean eventBean = customEventBean.getEventBean();
					sourceContext.collectWithTimestamp(eventBean, eventBean.getTimestamp());
				} else {
					EventTimerTask timerTask = new EventTimerTask(sourceContext, customEventBean);
					Timer timer = new Timer("EventTimer");
					timer.schedule(timerTask, processTimeDelay);
				}				
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
		
		private SourceContext<EventBean> sourceContext;
		private CustomEventBean customEventBean;
		
		public EventTimerTask(SourceContext<EventBean> sourceContext, CustomEventBean customEventBean) {
			this.sourceContext = sourceContext;
			this.customEventBean = customEventBean;
		}

		@Override
		public void run() {
			EventBean eventBean = customEventBean.getEventBean();
			sourceContext.collectWithTimestamp(eventBean, eventBean.getTimestamp());
		}
	}
	

	@Override
	public void cancel() {
		try {
			System.out.println("CustomSource.cancel()");
		} catch (Exception e) {
			
		} finally {
			
		}
	}
	
	private void close(Reader reader) {
		try {
			if (reader != null) {reader.close();}
		} catch (Exception e) {
		}
	}

}
