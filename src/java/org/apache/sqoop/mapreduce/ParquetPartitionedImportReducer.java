package org.apache.sqoop.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.SQLException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.sqoop.avro.AvroUtil;

import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;

@SuppressWarnings("deprecation")
public class ParquetPartitionedImportReducer extends
		SqoopReducer<Text, BytesWritable, GenericRecord, NullWritable> {

	private Schema schema = null;
	private boolean bigDecimalFormatString = true;
	private LargeObjectLoader lobLoader = null;
	private Class<SqoopRecord> recordClass;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		schema = ParquetJob.getAvroSchema(conf);
		bigDecimalFormatString = conf.getBoolean(
				ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT,
				ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT_DEFAULT);
		lobLoader = new LargeObjectLoader(conf, new Path(conf.get("sqoop.kite.lob.extern.dir", "/tmp/sqoop-parquet-" + context.getTaskAttemptID())));

		this.recordClass = (Class<SqoopRecord>) context.getConfiguration()
				.getClass("mapred.jar.record.class", SqoopRecord.class);
	}

	@Override
	protected void reduce(Text key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {

		try {

			for (BytesWritable bytesVal : values){
				GenericRecord genericRecord = getGenericRecord(bytesVal);
				
				if(genericRecord != null)
					context.write(genericRecord , null);
			}

		} catch (SQLException sqlE) {
			throw new IOException(sqlE);
		}

	}

	private GenericRecord getGenericRecord(BytesWritable bytesVal)
			throws IOException, InterruptedException, SQLException {

		SqoopRecord val = (SqoopRecord) ReflectionUtils.newInstance(
				recordClass, null);

		ByteArrayInputStream bis = new ByteArrayInputStream(bytesVal.getBytes());
		ObjectInput in = new ObjectInputStream(bis);

		val.readFields(in);

		in.close();
		bis.close();

		// Loading of LOBs was delayed until we have a Context.
		val.loadLargeObjects(lobLoader);

		return AvroUtil.toGenericRecord(val.getFieldMap(), schema,
				bigDecimalFormatString);
	}

	@Override
	protected void cleanup(Context context) throws IOException {
		if (null != lobLoader) {
			lobLoader.close();
		}
	}

}
