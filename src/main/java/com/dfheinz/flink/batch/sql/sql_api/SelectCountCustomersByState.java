package com.dfheinz.flink.batch.sql.sql_api;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.table.api.GroupedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

import akka.protobuf.WireFormat.FieldType;

public class SelectCountCustomersByState {
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
				    .field("id", Types.INT())
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
			
			// Step 4: Perform Operations
			// SELECT count(*)
			// FROM customers
			// GROUP BY state
			Table countAmountsByCustomer  = tableEnv.sqlQuery(
				"SELECT state, count(state) as stateCount FROM customers GROUP BY state");
					
			// Step 5: Write Results to Sink
			int parallelism = 1;
			TableSink<Row> sink = new CsvTableSink("output/count_customers_by_state.csv", ",", parallelism, WriteMode.OVERWRITE);
			countAmountsByCustomer.writeToSink(sink);
						
			// Step 6: Trigger Application Execution
			JobExecutionResult jobResult  =  env.execute("SelectCountCustomersByState");

		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
}
