package com.dfheinz.flink.tests;


import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.dfheinz.flink.test.streams.EventProducerStrategy;
import com.dfheinz.flink.test.streams.SocketProducerServer;



public class WatermarkTests {

	private static Logger logger = Logger.getLogger(WatermarkTests.class);


	@BeforeClass
	public static void setupClass() throws Exception {
	}

	@AfterClass
	public static void tearDownClass() throws Exception {
	}

	@Before
	public void setupTest() throws Exception {
	}

	@After
	public void tearDownTest() throws Exception {
	}

	@Test
	@Ignore
	public void testDummy() {
		logger.info("Dummy Test Begin");
	}
	
	@Test
	// @Ignore
	public void testStreamingEventTimeLate() {
		try {
			String fileName = "input/event_time_late.txt";
			runEventSocketProducer(fileName);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	@Test
	@Ignore
	public void testStreamingEventTimeIdeal() {
		try {
			String fileName = "input/event_time_ideal.txt";
			runEventSocketProducer(fileName);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	@Test
	@Ignore
	public void testStreaming102() {
		try {
			String fileName = "input/streaming102.txt";
			runEventSocketProducer(fileName);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	@Test
	@Ignore
	public void testStreaming131316() {
		try {
			System.out.println("testStreaming131316");
			String fileName = "input/streaming131316.txt";
			runEventSocketProducer(fileName);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	@Test
	@Ignore
	public void testStreaming131916() {
		try {
			System.out.println("testStreaming131916");
			String fileName = "input/streaming131916.txt";
			runEventSocketProducer(fileName);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	

	private void runEventSocketProducer(String fileName) throws Exception {
		String strategyClassName = EventProducerStrategy.class.getCanonicalName();
		Map<String,String> parms = new HashMap<String,String>();
		parms.put("filePath",fileName);
		SocketProducerServer server = new SocketProducerServer(strategyClassName, parms);
		server.execute();		
	}
	
	

}