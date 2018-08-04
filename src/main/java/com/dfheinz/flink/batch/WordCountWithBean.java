package com.dfheinz.flink.batch;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import com.dfheinz.flink.beans.WordCountBean;

public class WordCountWithBean {

	public static void main(String[] args) throws Exception {
		
		try {
	
			// Step 1: Get Execution Environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			ParameterTool parms = ParameterTool.fromArgs(args);
			env.getConfig().setGlobalJobParameters(parms);
			
			// Step 2: Get Data
			// DataSet<String> text = env.readTextFile(parms.get("input"));
			DataSet<String> text = env.readTextFile("file:///Flink/input/MyData.txt");
			
			// Step 3: Perform Transformations and Operations
			DataSet<WordCountBean> counts = text
				.flatMap(new LineSplitter())
				.groupBy("word")
				.reduce(new WordCountReducer());
					
			// Step 4: Write to Sink(s)
			counts.print();
			counts.writeAsText("file:///Flink/output/wordcount.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
			
			// Step 5: Trigger Execution
			JobExecutionResult result  =  env.execute("WordCountWithPOJO");
		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
	private static final class LineSplitter implements FlatMapFunction<String, WordCountBean> {
		@Override
		public void flatMap(String value, Collector<WordCountBean> collector) {
			String[] tokens = value.toLowerCase().split(" ");
			for (String token : tokens) {
				if (token.length() > 0) {
					collector.collect(new WordCountBean(token, 1));
				}
			}
		}
	}
	
	// WordCount Reducer
	public static class WordCountReducer implements ReduceFunction<WordCountBean> {
	  @Override
	  public WordCountBean reduce(WordCountBean bean1, WordCountBean bean2) {
	    return new WordCountBean(bean1.getWord(), bean1.getCount() + bean2.getCount());
	  }
	}
	
}
