package com.dfheinz.flink.batch.sql.sql_api;

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

public class SelectMovies {

	public static void main(String[] args) throws Exception {
		
		try {
			System.out.println("PetSQL BEGIN");
	
			// Get Execution Environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env); 
			
			final ParameterTool parms = ParameterTool.fromArgs(args);
			
	
			// Get and Set execution parameters.
			DataSet<String> text = null;
			if (!parms.has("input") || !parms.has("output")) {
				System.out.println("Usage --input to specify file input");
				System.out.println("Usage --output to specify file output");
				System.exit(1);
				return;
			}
			env.getConfig().setGlobalJobParameters(parms);
			
			// Echo our parameters
			System.out.println("input=" + parms.get("input"));
			System.out.println("output=" + parms.get("output"));
			
			// Get Source
			String inputPath = parms.get("input");
			CsvTableSource petsTableSource = CsvTableSource.builder()
				    .path(inputPath)
				    .ignoreFirstLine()
				    .fieldDelimiter(",")
				    .field("id", Types.INT())
				    .field("species", Types.STRING())
				    .field("color", Types.STRING())
				    .field("weight", Types.DOUBLE())
				    .field("name", Types.STRING())
				    .build();
			
			
			// Register our table source
			tableEnv.registerTableSource("pets", petsTableSource);

			
			// Perform Operations
			// SELECT species, count(species)
			// FROM pets
			// WHERE species = 'canine'
			// ORDER BY species
			Table counts = tableEnv.sqlQuery(
				"SELECT species, count(species)  FROM pets WHERE species <> 'bear' group by species");
			
			
			// Write Results to File
			int parallelism = 1;
			TableSink<Row> sink = new CsvTableSink(parms.get("output"), ",", parallelism, WriteMode.OVERWRITE);
			counts.writeToSink(sink);
			
					
			// Execute
			JobExecutionResult result  =  env.execute("PetSQL");

		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
	
	
}
