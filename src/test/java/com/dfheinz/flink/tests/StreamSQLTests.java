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


public class StreamSQLTests {

	private static Logger logger = Logger.getLogger(StreamSQLTests.class);


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
	public void testPageClicks1() {
		try {
			String fileName = "input/clickevents1.txt";
			runEventSocketProducer(fileName);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	@Test
	@Ignore
	public void testPageClicks2() {
		try {
			String fileName = "input/clickevents2.txt";
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