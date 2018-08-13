package com.dfheinz.flink.tests;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.dfheinz.flink.test.streams.EventReplayProducerStrategy;
import com.dfheinz.flink.test.streams.SocketProducerServer;
import com.dfheinz.flink.test.utils.Utils;



public class HistoryTests {

	private static Logger logger = Logger.getLogger(HistoryTests.class);


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
	public void testHistoricalReplay1() {
		try {
			String fileName = "input/historical1.txt";
			runEventSocketReplayProducer(fileName);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	@Test
	@Ignore
	public void testHistoricalReplay2() {
		try {
			String fileName = "input/historical2.txt";
			runEventSocketReplayProducer(fileName);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	@Test
	@Ignore
	public void testLongAdd() {
		try {
			long timestamp = 1534118996672L;
			long newValue = timestamp + 10000L;
			System.out.println("NEW VALUE=" + newValue);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	

	
	@Test
	@Ignore
	public void testReadFile() {
		try {
			String filePath = "input/history.txt";
			BufferedReader reader = null;
			InputStream is = getClass().getClassLoader().getResourceAsStream(filePath);
			if (is == null) {
				throw new Exception("File Not Found: " + filePath);
			}
			reader = new BufferedReader(new InputStreamReader(is));
			String line = null;
			while ((line=reader.readLine()) != null) {
				if (line.startsWith("#") || Utils.isBlank(line)) {
					continue;
				}
				// logger.info(line);
				String msg = parseLine(line);
				System.out.println(msg);
			}
			close(reader);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	private String parseLine(String line) {
		String[] tokens = line.split(",");
		String key = tokens[0];
		String label = tokens[1];
		String value = tokens[2];
		long timestamp = Long.valueOf(tokens[3]);
		String msg = String.format("%s %s %s %s", key, label, value, Utils.getFormattedTimestamp(timestamp));
		return msg;
	}
	
	private void close(Reader reader) {
		try {
			if (reader != null) {reader.close();}
		} catch (Exception e) {
		}
	}
	
	private void runEventSocketReplayProducer(String fileName) throws Exception {
		String strategyClassName = EventReplayProducerStrategy.class.getCanonicalName();
		Map<String,String> parms = new HashMap<String,String>();
		parms.put("filePath",fileName);
		SocketProducerServer server = new SocketProducerServer(strategyClassName, parms);
		server.execute();		
	}
	
	

}