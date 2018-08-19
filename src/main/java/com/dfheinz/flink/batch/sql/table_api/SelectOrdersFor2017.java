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

public class SelectOrdersFor2017 {

	public static void main(String[] args) throws Exception {
		
		try {
	
			// Get Execution Environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env); 
			ParameterTool parms = ParameterTool.fromArgs(args);
			env.getConfig().setGlobalJobParameters(parms);
			String input = "input/batch/orders.csv";
			
			// Get Source
			CsvTableSource orderTableSource = CsvTableSource.builder()
				    .path(input)
				    .ignoreFirstLine()
				    .fieldDelimiter(",")
				    .field("id", Types.INT())
				    .field("order_date", Types.SQL_DATE())
				    .field("amount", Types.DECIMAL())
				    .field("customer_id", Types.LONG())
				    .build();
			
			
			// Register our table source
			tableEnv.registerTableSource("orders", orderTableSource);
			Table orderTable = tableEnv.scan("orders");

			
			// Perform Operations
			// SELECT *
			// FROM orders
			// STRING.like(STRING)
			Table selectAllOrders = orderTable
				.select("id, order_date, amount, customer_id")
				// .filter("amount > 35.00");
			    // .filter("amount > 28.00 && amount < 114.00");
			    //.filter("amount == 28.15 || amount == 112.88");
			    // .filter("order_date < '2018-08-10'.toDate()");
				.filter("order_date < '2018-01-01'.toDate()");
				// .filter("order_date < currentDate()");
			
			
			// Write Results to File
			int parallelism = 1;
			TableSink<Row> sink = new CsvTableSink("output/select_all_orders_2017.csv", ",", parallelism, WriteMode.OVERWRITE);
			selectAllOrders.writeToSink(sink);
						
			// Execute
			JobExecutionResult result  =  env.execute("SelectOrders");

		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
}
