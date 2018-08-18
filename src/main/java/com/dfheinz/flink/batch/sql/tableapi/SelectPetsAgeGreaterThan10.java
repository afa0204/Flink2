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

public class SelectPetsAgeGreaterThan10 {

	public static void main(String[] args) throws Exception {
		
		try {
	
			// Get Execution Environment
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env); 
			ParameterTool parms = ParameterTool.fromArgs(args);
			env.getConfig().setGlobalJobParameters(parms);
			String input = "input/batch/pets.csv";
			String output = "output/select_pets_age_gt_10.csv";
			
			// Get Source
			CsvTableSource petsTableSource = CsvTableSource.builder()
				    .path(input)
				    .ignoreFirstLine()
				    .fieldDelimiter(",")
				    .field("id", Types.INT())
				    .field("species", Types.STRING())
				    .field("color", Types.STRING())
				    .field("weight", Types.DOUBLE())
				    .field("name", Types.STRING())
				    .field("age", Types.INT())
				    .build();
			
			
			// Register our table source
			tableEnv.registerTableSource("pets", petsTableSource);
			Table pets = tableEnv.scan("pets");

			
			// Perform Operations
			// SELECT species, count(species)
			// FROM pets
			// WHERE species != 'bear'
			// GROUP BY species
			Table counts = pets
			        .select("id,species,color,weight,name,age")
			        .filter("age > 10");
			
			// Write Results to File
			int parallelism = 1;
			TableSink<Row> sink = new CsvTableSink(output, ",", parallelism, WriteMode.OVERWRITE);
			counts.writeToSink(sink);
			
					
			// Execute
			JobExecutionResult result  =  env.execute("PetTable");

		
		} catch (Exception e) {
			System.out.println("ERROR:\n" + e);
		}
	}
	
	public static class PetCount {
		public String species;
		public long count;
		
		public String toString() {
			StringBuilder buffer = new StringBuilder();
			buffer.append("PetCount.species:" + species);
			buffer.append("\tPetCount.count:" + count);
			return buffer.toString();
		}
		
	}
	
	
}
