package com.dfheinz.flink.stream.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;

public class PageViewUpsertSink implements UpsertStreamTableSink<Row> {

	@Override
	public TableSink<Tuple2<Boolean, Row>> configure(String[] arg0, TypeInformation<?>[] arg1) {
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
	public void emitDataStream(DataStream<Tuple2<Boolean, Row>> arg0) {
		arg0.print();
	}

	@Override
	public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
		return null;
	}

	@Override
	public TypeInformation<Row> getRecordType() {
		return null;
	}

	@Override
	public void setIsAppendOnly(Boolean arg0) {
		
	}

	@Override
	public void setKeyFields(String[] arg0) {
		
	}


	

}
