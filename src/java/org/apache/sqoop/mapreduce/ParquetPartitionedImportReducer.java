package org.apache.sqoop.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.sqoop.avro.AvroUtil;

import com.cloudera.sqoop.lib.SqoopRecord;

@SuppressWarnings("deprecation")
public class ParquetPartitionedImportReducer extends
		SqoopReducer<Text, SqoopRecord, GenericRecord, NullWritable> {

	private Schema schema = null;
	private boolean bigDecimalFormatString = true;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		schema = ParquetJob.getAvroSchema(conf);
		bigDecimalFormatString = conf.getBoolean(
				ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT,
				ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT_DEFAULT);
		
	}

	@Override
	protected void reduce(Text key, Iterable<SqoopRecord> values,
			Context context) throws IOException, InterruptedException {

		for (SqoopRecord val : values){				
			context.write(AvroUtil.toGenericRecord(val.getFieldMap(), schema,
						bigDecimalFormatString), null);
		}

	}

}
