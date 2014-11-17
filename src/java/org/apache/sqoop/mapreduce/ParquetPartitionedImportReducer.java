package org.apache.sqoop.mapreduce;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.sqoop.avro.AvroUtil;

import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;

@SuppressWarnings("deprecation")
public class ParquetPartitionedImportReducer extends
		SqoopReducer<IntWritable, SqoopRecord, GenericRecord, NullWritable> {

	private Schema schema = null;
	private boolean bigDecimalFormatString = true;
	private LargeObjectLoader lobLoader = null;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		schema = ParquetJob.getAvroSchema(conf);
		bigDecimalFormatString = conf.getBoolean(
				ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT,
				ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT_DEFAULT);
		
		lobLoader = new LargeObjectLoader(conf, new Path(conf.get("sqoop.kite.lob.extern.dir", "/tmp/sqoop-parquet-" + context.getTaskAttemptID())));
	}

	@Override
	protected void reduce(IntWritable key, Iterable<SqoopRecord> values,
			Context context) throws IOException, InterruptedException {

		try {

			for (SqoopRecord val : values){
				// Loading of LOBs was delayed until we have a Context.
				val.loadLargeObjects(lobLoader);
				
				context.write(AvroUtil.toGenericRecord(val.getFieldMap(), schema,
							bigDecimalFormatString), null);
			}

		} catch (SQLException sqlE) {
			throw new IOException(sqlE);
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException {
		if (null != lobLoader) {
			lobLoader.close();
		}
	}

}
