/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.mapreduce.AutoProgressMapper;

/**
 * Imports records by writing them to a Parquet File.
 */
public class ParquetPartitionedImportMapper extends
		AutoProgressMapper<LongWritable, SqoopRecord, Text, BytesWritable> {

	private static final int DIFF_HOURS = 0;
	
	private static final long MILLISECONDS_TO_ADD = DIFF_HOURS * 60 * 60 * 1000;
	
	private Calendar cal;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		this.cal = Calendar.getInstance(TimeZone.getTimeZone("Etc/GMT-2"));
	}
	
	protected void map(
			LongWritable key,
			SqoopRecord val,
			Context context)
			throws IOException, InterruptedException {
		
		//Get timestamp from record
		Timestamp timestamp = (Timestamp) val.getFieldMap().get("UTC_STAMP");
		this.cal.setTimeInMillis(timestamp.getTime());
		
		//Get partition key
		Text keyOut = getKey(val);
		
		context.write(keyOut, getBytes(val));
	}

	private BytesWritable getBytes(SqoopRecord val) throws IOException {

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(bos);
		
		val.write(out);
		
		out.close();
		bos.close();	
		
		return new BytesWritable(bos.toByteArray());
	}

	private void applyTimeDiff(Timestamp val) {
		long milliseconds = val.getTime() + MILLISECONDS_TO_ADD;
		int nanoseconds = val.getNanos();
		
		val.setTime(milliseconds);
		val.setNanos(nanoseconds);
	}

	private Text getKey(SqoopRecord val) {
		return new Text(this.cal.get(Calendar.YEAR) + "-"
				+ this.cal.get(Calendar.MONTH) + "-"
				+ this.cal.get(Calendar.DAY_OF_MONTH) + " "
				+ this.cal.get(Calendar.HOUR_OF_DAY));
	}

}
