package com.dfheinz.flink.batch.sql.table_api;

import org.apache.flink.api.common.JobExecutionResult;
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

public class UnionCustomers {

	public static void main(String[] args) throws Exception {
		
		try {
	
			// Step 1: Get Execution Environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
			int parallelism = 1;
			ParameterTool parms = ParameterTool.fromArgs(args);
			env.getConfig().setGlobalJobParameters(parms);
			
			// Step 2: Get Table Source
			CsvTableSource customerTableSource1 = CsvTableSource.builder()
				    .path("input/batch/customers.csv")
				    .ignoreFirstLine()
				    .fieldDelimiter(",")
				    .field("id", Types.LONG())
				    .field("first_name", Types.STRING())
				    .field("last_name", Types.STRING())
				    .field("street_address1", Types.STRING())
				    .field("city", Types.STRING())
				    .field("state", Types.STRING())
				    .field("zip", Types.STRING())
				    .build();
			
			// Step 2: Get Table Source
			CsvTableSource customerTableSource2 = CsvTableSource.builder()
				    .path("input/batch/customers_canada.csv")
				    .ignoreFirstLine()
				    .fieldDelimiter(",")
				    .field("id", Types.LONG())
				    .field("first_name", Types.STRING())
				    .field("last_name", Types.STRING())
				    .field("street_address1", Types.STRING())
				    .field("city", Types.STRING())
				    .field("state", Types.STRING())
				    .field("zip", Types.STRING())
				    .build();
			
			
			// Step 3: Register our table source
			tableEnv.registerTableSource("customerSet1", customerTableSource1);
			Table customerTable1 = tableEnv.scan("customerSet1");
			
			tableEnv.registerTableSource("customerSet2", customerTableSource2);
			Table customerTable2 = tableEnv.scan("customerSet2");
			
			// Step 4: Perform Operations
			Table allCustomers = customerTable1.union(customerTable2);
								
			// Step 5: Write Results to Sink
			TableSink<Row> sink = new CsvTableSink("output/customer_union.csv", ",", parallelism, WriteMode.OVERWRITE);
			allCustomers.writeToSink(sink);
					
			// Step 6: Trigger Application Execution
			JobExecutionResult result  =  env.execute("UnionCustomers");

		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
}
