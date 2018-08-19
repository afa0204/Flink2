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

public class SelectOrderStatus100 {
	
	public static void main(String[] args) throws Exception {
		try {
			// Step 1: Get Execution Environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
			int parallelism = 1;
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
				    .field("status", Types.LONG())
				    .field("customer_id", Types.LONG())
				    .build();		
			
			// Step 3: Register our table source
			tableEnv.registerTableSource("orders", orderTableSource);
			Table orderTable = tableEnv.scan("orders");
			
			// Step 4: Perform Operations
			// SELECT *
			// FROM orders
			Table orderStatus100 = orderTable
				.select("id,order_date,amount,status,customer_id")
				.filter("status = 100");
								
			// Step 5: Write Results to Sink
			TableSink<Row> sink = new CsvTableSink("output/select_orders_status100.csv", ",", parallelism, WriteMode.OVERWRITE);
			orderStatus100.writeToSink(sink);
					
			// Step 6: Trigger Application Execution
			JobExecutionResult result  =  env.execute("SelectOrderStatus100");
		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
}
