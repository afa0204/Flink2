package com.dfheinz.flink.batch.sql.table_api;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

public class IntersectCustomers {

	public static void main(String[] args) throws Exception {
		
		try {
			System.out.println("InnerJoin BEGIN");
	
			// Get Execution Environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env); 
			
			final ParameterTool parms = ParameterTool.fromArgs(args);
			env.getConfig().setGlobalJobParameters(parms);
			
			
			// Customer Set 1
			// Register our table source
			String customersPath1 = "input/customerset1.csv";
			CsvTableSource customerSet1Source = CsvTableSource.builder()
				    .path(customersPath1)
				    .ignoreFirstLine()
				    .fieldDelimiter(",")
				    .field("customer_id", Types.INT())
				    .field("first_name", Types.STRING())
				    .field("last_name", Types.STRING())
				    .field("email", Types.STRING())
				    .field("address", Types.STRING())
				    .field("city", Types.STRING())
				    .field("state", Types.STRING())
				    .field("zip", Types.STRING())
				    .build();
			tableEnv.registerTableSource("customerset1", customerSet1Source);
			Table customerset1 = tableEnv.scan("customerset1");
			
			
			// Customer Set 2
			// Register our table source
			String customersPath2 = "input/customerset2.csv";
			CsvTableSource customerSet2Source = CsvTableSource.builder()
				    .path(customersPath2)
				    .ignoreFirstLine()
				    .fieldDelimiter(",")
				    .field("customer_id", Types.INT())
				    .field("first_name", Types.STRING())
				    .field("last_name", Types.STRING())
				    .field("email", Types.STRING())
				    .field("address", Types.STRING())
				    .field("city", Types.STRING())
				    .field("state", Types.STRING())
				    .field("zip", Types.STRING())
				    .build();
			tableEnv.registerTableSource("customerset2", customerSet2Source);
			Table customerset2 = tableEnv.scan("customerset2");
					
			// Perform Intersection
			Table commonCustomers = customerset1.intersect(customerset2);
			
			
			// Write to Sinks
			int parallelism = 1;
			
			// Write to Sinks
			commonCustomers.printSchema();
			DataSet<Row> result = tableEnv.toDataSet(commonCustomers, Row.class);
			result.print();
			
			TableSink<Row> allSink = new CsvTableSink("output/intersect_customers.csv", ",", parallelism, WriteMode.OVERWRITE);
			commonCustomers.writeToSink(allSink);
				
					
			// Execute
			JobExecutionResult jobResult  =  env.execute("Union");

		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
	
	
}
