package com.dfheinz.flink.batch.sql.sqlapi;

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

public class SelectAllOrders {

	public static void main(String[] args) throws Exception {
		
		try {	
			// Step 1: Get Execution Environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			int parallelism = 1;
			BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env); 
			ParameterTool parms = ParameterTool.fromArgs(args);
			env.getConfig().setGlobalJobParameters(parms);
			
			// Step 2: Get Table Source
			CsvTableSource orderTableSource = CsvTableSource.builder()
					.path("input/batch/orders.csv")
				    .ignoreFirstLine()
				    .fieldDelimiter(",")
				    .field("id", Types.LONG())
				    .field("order_date", Types.SQL_DATE())
				    .field("amount", Types.DECIMAL())
				    .field("customer_id", Types.LONG())
				    .build();
			
			// Step 3: Register our table source
			tableEnv.registerTableSource("orders", orderTableSource);
		
			// Step 4: Perform Operations
			// SELECT id, order_date, amount, customer_id
			// FROM orders
			Table allOrders = tableEnv.sqlQuery(
				"SELECT id, order_date, amount, customer_id FROM orders ORDER BY id");
			
			// Step 5: Write Results to Sink
			TableSink<Row> sink = new CsvTableSink("output/select_all_orders_api_sql.csv", ",", parallelism, WriteMode.OVERWRITE);
			allOrders.writeToSink(sink);
							
			// Execute
			JobExecutionResult result  =  env.execute("SelectAllOrders");
		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
	
	
}
