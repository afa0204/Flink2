package com.dfheinz.flink.batch.sql.table_api;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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

public class SelectCustomersLike {

	public static void main(String[] args) throws Exception {
		
		try {
	
			// Step 1: Get Execution Environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env); 
			ParameterTool parms = ParameterTool.fromArgs(args);
			env.getConfig().setGlobalJobParameters(parms);
			
			
			// Step 2: Get Table Source
			CsvTableSource customerTableSource = CsvTableSource.builder()
				    .path("input/batch/customers.csv")
				    .ignoreFirstLine()
				    .fieldDelimiter(",")
				    .field("id", Types.LONG())
				    .field("first_name", Types.STRING())
				    .field("last_name", Types.STRING())
				    .field("country",Types.STRING())
				    .field("street_address1", Types.STRING())
				    .field("city", Types.STRING())
				    .field("state", Types.STRING())
				    .field("zip", Types.STRING())
				    .build();
					
			// Step 3: Register our table source
			tableEnv.registerTableSource("customers", customerTableSource);
			Table customerTable = tableEnv.scan("customers");
	
			// Step 4: Perform Operations
			// Perform Operations
			// SELECT first_name, last_name, state
			// FROM customers
			// WHERE last_name like 'Green%'
			Table result = customerTable
				.select("first_name,last_name,state")
				.filter("last_name.like('Green%')");
			
			// Step 5: Write Results to Sink
			int parallelism = 1;
			TableSink<Row> sink = new CsvTableSink("output/customers_like_green.csv", ",", parallelism, WriteMode.OVERWRITE);
			result.writeToSink(sink);
		
					
			// Step 6: Trigger Application Execution
			JobExecutionResult jobResult  =  env.execute("SelectCustomersLike");

		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
}
