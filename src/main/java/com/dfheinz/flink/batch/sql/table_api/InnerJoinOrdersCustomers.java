package com.dfheinz.flink.batch.sql.table_api;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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

public class InnerJoinOrdersCustomers {
	
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
				    .field("order_id", Types.LONG())
				    .field("order_date", Types.SQL_DATE())
				    .field("amount", Types.DECIMAL())
				    .field("status", Types.LONG())
				    .field("customer_key", Types.LONG())
				    .build();
			
			CsvTableSource customerTableSource = CsvTableSource.builder()
				    .path("input/batch/customers.csv")
				    .ignoreFirstLine()
				    .fieldDelimiter(",")
				    .field("customer_id", Types.LONG())
				    .field("first_name", Types.STRING())
				    .field("last_name", Types.STRING())
				    .field("country",Types.STRING())
				    .field("street_address1", Types.STRING())
				    .field("city", Types.STRING())
				    .field("state", Types.STRING())
				    .field("zip", Types.STRING())
				    .build();
			
			// Step 3: Register our table sources
			tableEnv.registerTableSource("orders", orderTableSource);
			Table orders = tableEnv.scan("orders");
			
			tableEnv.registerTableSource("customers", customerTableSource);
			Table customers = tableEnv.scan("customers");
			
			// Step 4: Perform Operations
			Table innerJoin = orders.join(customers).where("customer_key=customer_id").select("order_id,last_name,order_date,amount");
								
			// Step 5: Write Results to Sink
			TableSink<Row> sink = new CsvTableSink("output/orders_join_customers.csv", ",", parallelism, WriteMode.OVERWRITE);
			innerJoin.writeToSink(sink);
					
			// Step 6: Trigger Application Execution
			JobExecutionResult result  =  env.execute("InnerJoinOrdersCustomers");
		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
}
