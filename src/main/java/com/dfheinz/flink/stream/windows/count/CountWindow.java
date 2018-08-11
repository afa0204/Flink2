package com.dfheinz.flink.stream.windows.count;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;



public class CountWindow {

	public static void main(String[] args) throws Exception {		
	
		// Get and Set execution parameters.
		ParameterTool parms = ParameterTool.fromArgs(args);
		if (!parms.has("host") || 
			!parms.has("port")) {
			System.out.println("Usage --host to specify host");
			System.out.println("Usage --port to specify port");
			System.exit(1);
			return;
		}		
		
		// Set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(parms);
		
		// Get Our Data Stream 
		String host1 = parms.get("host");
		int port1 = parms.getInt("port");
		DataStream<String> dataStream = env.socketTextStream(host1,port1);
		
		WindowedStream<Course,Tuple,GlobalWindow> windowedStream = dataStream
		  .map(new RowParser())
		  .keyBy("staticKey")
		  .countWindow(3);
		
		// DataStream<Map<String,Integer>> usCourses = windowedStream.apply(new MyCourseFunction());
		// usCourses.print();
		
		DataStream<Map<String,Integer>> usCourses2 = windowedStream.process(new MyProcessCourseFunction());
		usCourses2.print();
					
		
		// Execute
		JobExecutionResult result  =  env.execute("CountWindow");
	}
	
	
	public static class MyCourseFunction implements WindowFunction<Course,Map<String,Integer>,Tuple,GlobalWindow> {

		@Override
		public void apply(Tuple key, GlobalWindow window, Iterable<Course> input, Collector<Map<String, Integer>> collector) throws Exception {
			Map<String,Integer> courseMap = new HashMap<String,Integer>();
			for (Course nextCourse : input) {
				if (nextCourse.getCountry().toLowerCase().equals("usa")) {
					Integer entry = courseMap.get(nextCourse.getCourseName());
					if (entry == null) {
						courseMap.put(nextCourse.getCourseName(), new Integer(1));
					} else {
						courseMap.put(nextCourse.getCourseName(), entry+1);
					}
				}
			}
			collector.collect(courseMap);
		}
	}
	
	
	private static class MyProcessCourseFunction extends ProcessWindowFunction<Course, Map<String,Integer>, Tuple, GlobalWindow> {

		private static final long serialVersionUID = 1L;
		
		public MyProcessCourseFunction() {
		}
		
		@Override
		public void process(Tuple key,
				Context context,
				Iterable<Course> inputElements,
				Collector<Map<String,Integer>> collector) throws Exception {
			
			Map<String,Integer> courseMap = new HashMap<String,Integer>();
			for (Course nextCourse : inputElements) {
				if (nextCourse.getCountry().toLowerCase().equals("usa")) {
					Integer entry = courseMap.get(nextCourse.getCourseName());
					if (entry == null) {
						courseMap.put(nextCourse.getCourseName(), new Integer(1));
					} else {
						courseMap.put(nextCourse.getCourseName(), entry+1);
					}
				}
				
			}
			collector.collect(courseMap);			

		}
	}	
	
	
	
	// Input Line
	// biology,USA
	// calculus,Germany
	// chemistry,USA
	private static class RowParser implements MapFunction<String,Course> {
		@Override
		public Course map(String input) throws Exception {
			String[] elements = input.split(",");
			String course = elements[0];
			String country = elements[1];
			return new Course(course,country,new Integer(1));
		}
	}
	
	public static class Course {
		private Integer staticKey = -1;
		private String courseName;
		private String country;
		private Integer count;
		
		public Course() {
		}
		
		public Course(String courseName, String country, Integer count) {
			this.courseName = courseName;
			this.country = country;
			this.count = count;
		}

		public Integer getStaticKey() {
			return staticKey;
		}

		public void setStaticKey(Integer staticKey) {
			this.staticKey = staticKey;
		}

		public String getCourseName() {
			return courseName;
		}

		public void setCourseName(String courseName) {
			this.courseName = courseName;
		}

		public String getCountry() {
			return country;
		}

		public void setCountry(String country) {
			this.country = country;
		}

		public Integer getCount() {
			return count;
		}

		public void setCount(Integer count) {
			this.count = count;
		}
		
		
		
	}
	
	
	
}
