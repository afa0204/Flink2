package com.dfheinz.flink.batch.sql.tableapi;

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

public class LeftOuterJoin {

	public static void main(String[] args) throws Exception {
		
		try {
			System.out.println("LeftOuterJoin BEGIN");
	
			// Get Execution Environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env); 
			
			final ParameterTool parms = ParameterTool.fromArgs(args);
			env.getConfig().setGlobalJobParameters(parms);
			
			
			// Get Customers
			String customersPath = "input/customers.csv";
			// id,first_name,last_name,email,address,city,state,zip
			CsvTableSource customersTableSource = CsvTableSource.builder()
				    .path(customersPath)
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
			// Register our table source
			tableEnv.registerTableSource("customers", customersTableSource);
			Table customers = tableEnv.scan("customers");
					
			// Get Orders
			String ordersPath = "input/orders.csv";
			// order_id,order_date,amount,customer_id
			CsvTableSource ordersTableSource = CsvTableSource.builder()
				    .path(ordersPath)
				    .ignoreFirstLine()
				    .fieldDelimiter(",")
				    .field("order_id", Types.INT())
				    .field("order_date", Types.SQL_DATE())
				    .field("amount", Types.DECIMAL())
				    .field("customer_key", Types.INT())
				    .build();			
			
			// Register our table source
			tableEnv.registerTableSource("orders", ordersTableSource);
			Table orders = tableEnv.scan("orders");
	
			// Perform Join
			Table leftOuterJoin = customers.leftOuterJoin(orders,"customer_id=customer_key").select("first_name,last_name,order_date,amount");
		
			// Write to Sinks
			int parallelism = 1;
			
			// Write to Sinks
			leftOuterJoin.printSchema();
//			DataSet<Row> result = tableEnv.toDataSet(leftOuterJoin, Row.class);
//			result.print();
			
			TableSink<Row> joinSink = new CsvTableSink("output/leftouterjoin.csv", ",", parallelism, WriteMode.OVERWRITE);
			leftOuterJoin.writeToSink(joinSink);
				
					
			// Execute
			JobExecutionResult result  =  env.execute("LeftOuterJoin");

		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
	
	
}
