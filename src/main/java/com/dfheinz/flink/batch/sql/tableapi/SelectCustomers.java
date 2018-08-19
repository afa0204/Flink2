package com.dfheinz.flink.batch.sql.tableapi;

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

public class SelectCustomers {

	public static void main(String[] args) throws Exception {
		
		try {
	
			// Get Execution Environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env); 
			ParameterTool parms = ParameterTool.fromArgs(args);
			env.getConfig().setGlobalJobParameters(parms);
			String input = "input/batch/customers.csv";
			String output = "output/select_all_customers.csv";
			
			// 1001,Fred,Jenkins,USA,100 Main Street,Reston,VA,20194
			
			
			// Get Source
			CsvTableSource customerTableSource = CsvTableSource.builder()
				    .path(input)
				    .ignoreFirstLine()
				    .fieldDelimiter(",")
				    .field("id", Types.INT())
				    .field("first_name", Types.STRING())
				    .field("last_name", Types.STRING())
				    .field("street_address1", Types.STRING())
				    .field("city", Types.STRING())
				    .field("state", Types.STRING())
				    .field("zip", Types.STRING())
				    .build();
			
			
			// Register our table source
			tableEnv.registerTableSource("customers", customerTableSource);
			Table customerTable = tableEnv.scan("customers");

			
			// Perform Operations
			// SELECT *
			// FROM customers
			Table selectAllCustomers = customerTable
				.select("*");
			
			// Perform Operations
			// SELECT *
			// FROM customers
			Table selectCustomers = customerTable
				.select("last_name,state");
			
			selectAllCustomers.printSchema();
			
			// Write Results to File
			int parallelism = 1;
			TableSink<Row> sink = new CsvTableSink(output, ",", parallelism, WriteMode.OVERWRITE);
			// TableSink sink = new CsvTableSink(output, ",", parallelism, WriteMode.OVERWRITE);
			// customerTable.writeToSink(sink);
			selectCustomers.writeToSink(sink);
		
			// Register Table Sink
//			TableSink csvSink = new CsvTableSink("output/output_orders.csv", ",", parallelism, WriteMode.OVERWRITE);
//			String[] fieldNames = {"id", "first_name", "last_name", "address","city","state","zip"};
//			TypeInformation[] fieldTypes = {Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()};
//			tableEnv.registerTableSink("outputOrders", fieldNames, fieldTypes, csvSink);
//			selectAllCustomers.insertInto("outputOrders");
			
					
			// Execute
			JobExecutionResult result  =  env.execute("SelectCustomers");

		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
}
