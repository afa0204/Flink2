package com.dfheinz.flink.test.streams;

public interface ProducerStrategy {
	
	public void execute() throws Exception;
	
	public void shutdown();
	
}
