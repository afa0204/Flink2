package com.dfheinz.flink.tests;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.dfheinz.flink.test.utils.Utils;



public class CoreTests {

	private static Logger logger = Logger.getLogger(CoreTests.class);


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
	public void testTimeBoundary() {
		try {
			long startTime = System.currentTimeMillis();
			// long startTime = 1534205809604L;
			System.out.println("StartTime=" + startTime + " " +  Utils.getFormattedTimestamp(startTime));
			
			long adjusted = (((startTime-5000)/5000)*5000L) + 5000L;
			System.out.println("Adjusted=" + adjusted + " " + Utils.getFormattedTimestamp(adjusted));
			
			
			// long adjusted = startTime + 5000L;
			// long adjusted = 1534205809000L;
			// long boundary = 5;
			// long adjusted = (startTime + (5*1000)) % (5*1000);
			// System.out.println("Adjusted=" + adjusted + " " + Utils.getFormattedTimestamp(adjusted));
			
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
	public void testConvertDateToTimestamp() {
		try {
			// 1534118982664 = 08/12/2018 20:09:42
			// long timestamp = 1534118982664L;
			// System.out.println("Timestamp=" + Utils.getFormattedTimestamp(timestamp));
			String timestampValue = "08/12/2018 20:09:42";
			long timestamp = Utils.getTimestamp(timestampValue);
			System.out.println("timestamp=" + timestamp);
			
			System.out.println("Original Time=" + Utils.getFormattedTimestamp(timestamp));
			
			
		} catch (Exception e) {
			logger.error("ERROR", e);
		}		
	}
	
	@Test
	@Ignore
	public void testReadFile() {
		try {
			String filePath = "input/historical3.txt";
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
		long timestamp = Utils.getTimestamp(tokens[3]);
		String msg = String.format("%s %s %s %d %s", key, label, value, timestamp, Utils.getFormattedTimestamp(timestamp));
		return msg;
	}
	
	private void close(Reader reader) {
		try {
			if (reader != null) {reader.close();}
		} catch (Exception e) {
		}
	}
	

}