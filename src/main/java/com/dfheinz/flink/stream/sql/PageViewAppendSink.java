package com.dfheinz.flink.stream.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

public class PageViewAppendSink implements AppendStreamTableSink<Row> {

	@Override
	public TableSink<Row> configure(String[] arg0, TypeInformation<?>[] arg1) {
		return null;
	}

	@Override
	public String[] getFieldNames() {
		return null;
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return null;
	}

	@Override
	public TypeInformation<Row> getOutputType() {
		return null;
	}

	@Override
	public void emitDataStream(DataStream<Row> arg0) {
		arg0.print();
	}
	

}
