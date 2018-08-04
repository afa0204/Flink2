package com.dfheinz.flink.beans;

public class WordCountPOJO {

	public String word;
	public int count;
	
	public WordCountPOJO() {
	}
	
	public WordCountPOJO(String word, int count) {
		this.word = word;
		this.count = count;
	}
	
	public String toString() {
		return word + ":" + count;
	}
	
}
