package com.dfheinz.flink.test.streams;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

import org.apache.log4j.Logger;


public class SocketProducerServer {

	private static Logger logger = Logger.getLogger(SocketProducerServer.class);
	private String strategyClassName;
	private Map<String,String> strategyParms;
	
	public SocketProducerServer(String strategyClassName, Map<String,String> strategyParms) {
		this.strategyClassName = strategyClassName;
		this.strategyParms = strategyParms;
	}
	
	public void execute() {
		try {
			logger.info("SocketProducerServer Startup...");
			int portNumber = 9999;
			ServerSocket serverSocket = new ServerSocket(portNumber);
			
			while (true) {
				logger.info("Waiting for Client...");
				Socket clientSocket = serverSocket.accept();
				logger.info("Got client connection");
				
				// Create Strategy
				String filePath = strategyParms.get("filePath");
				Constructor constructor = Class.forName(strategyClassName).getConstructor(java.lang.String.class);
				SocketProducerStrategy strategy = (SocketProducerStrategy)constructor.newInstance(filePath);
				PrintWriter socketOutput = new PrintWriter(clientSocket.getOutputStream(), true);
				BufferedReader socketInput = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
				strategy.setSocketWriter(socketOutput);
				
				// Start Data Producing Thread
				ProducerThread producer = new ProducerThread(strategy);
				producer.start();					
			}	
		} catch (Exception e) {
			logger.info("SocketProducerServer Shutdown");
		}
	}

	
	private static class ProducerThread extends Thread {
		
		private ProducerStrategy strategy;
		private boolean stop = false;

		
		public ProducerThread(ProducerStrategy strategy) {
			this.strategy = strategy;
		}
		
		
		public void run() {
			try {
				strategy.execute();
			} catch (Exception e) {
				logger.error("ERROR", e);
			} finally {
			}
		}	

		private void shutdown() {
			strategy.shutdown();
		}
		
	};
	
	
	public static void main(String[] args) {
		try {
			
		} catch (Exception e) {
			logger.error("Error", e);
		} finally {
			// stdin.close();
		}
	}
	
	

}
