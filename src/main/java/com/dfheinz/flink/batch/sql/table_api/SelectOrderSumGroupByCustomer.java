package com.dfheinz.flink.batch.sql.table_api;

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

public class SelectOrderSumGroupByCustomer {
	public static void main(String[] args) throws Exception {
		try {
	
			// Get Execution Environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env); 
			ParameterTool parms = ParameterTool.fromArgs(args);
			env.getConfig().setGlobalJobParameters(parms);
			
			// Get Source
			CsvTableSource orderTableSource = CsvTableSource.builder()
				    .path("input/batch/orders.csv")
				    .ignoreFirstLine()
				    .fieldDelimiter(",")
				    .field("id", Types.LONG())
				    .field("order_date", Types.SQL_DATE())
				    .field("amount", Types.DECIMAL())
				    .field("status", Types.DECIMAL())
				    .field("customer_key", Types.LONG())
				    .build();
			
			
			// Register our table source
			tableEnv.registerTableSource("orders", orderTableSource);
			Table orderTable = tableEnv.scan("orders");

			
			// Perform Operations
			// SELECT customer_key, sum(amount) as totalAmount
			// FROM orders
			// GROUP BY CUSTOMER_KEY
			Table amountsByCustomer = orderTable
				.groupBy("customer_key")
				.select("customer_key, amount.sum as totalAmount");
					
			// Write Results to File
			int parallelism = 1;
			TableSink<Row> sink = new CsvTableSink("output/amounts_by_customer.csv", ",", parallelism, WriteMode.OVERWRITE);
			amountsByCustomer.writeToSink(sink);
						
			// Execute
			JobExecutionResult jobResult  =  env.execute("SelectOrderSumGroupByCustomer");

		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
}
