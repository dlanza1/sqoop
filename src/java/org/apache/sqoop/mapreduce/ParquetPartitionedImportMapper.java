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

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.sqoop.lib.SqoopRecord;
import org.kitesdk.data.spi.partition.CalendarFieldPartitioner;
import org.kitesdk.data.spi.partition.DayOfMonthFieldPartitioner;
import org.kitesdk.data.spi.partition.HourFieldPartitioner;
import org.kitesdk.data.spi.partition.MonthFieldPartitioner;
import org.kitesdk.data.spi.partition.YearFieldPartitioner;

import com.cloudera.sqoop.lib.LargeObjectLoader;

/**
 * Imports records by writing them to a Parquet File.
 */
public class ParquetPartitionedImportMapper extends
		AutoProgressMapper<LongWritable, SqoopRecord, Text, SqoopRecord> {

	private static int module;
	
	private LargeObjectLoader lobLoader = null;

	private String field_part_module;
	private String field_part_year;
	private String field_part_month;
	private String field_part_day;
	private String field_part_hour;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
				
		getPartitioningFileds(conf);
		
		lobLoader = new LargeObjectLoader(conf, new Path(conf.get("sqoop.kite.lob.extern.dir", "/tmp/sqoop-parquet-" + context.getTaskAttemptID())));
	}
	
	private void getPartitioningFileds(Configuration conf) {
		field_part_module = conf.get("partitioning.module.field");
		if(module != -1 && field_part_module != null){
			module = conf.getInt("partitioning.module", Integer.MAX_VALUE);
		}

		field_part_year = conf.get("partitioning.year.field");
		field_part_month = conf.get("partitioning.month.field");
		field_part_day = conf.get("partitioning.day.field");
		field_part_hour = conf.get("partitioning.hour.field");
	}

	protected void map(
			LongWritable key,
			SqoopRecord val,
			Context context)
			throws IOException, InterruptedException {
		
		// Loading of LOBs was delayed until we have a Context.
		try {
			val.loadLargeObjects(lobLoader);
			
			context.write(getOutputKey(val), val);
		} catch (SQLException e) {
			e.printStackTrace();

			throw new IOException(e);
		}
		
	}

	private Text getOutputKey(SqoopRecord val) {
		
		String key = new String();
		
		if(module != -1 && field_part_module != null){
			key = key + "-" + val.getFieldMap().get(field_part_module);
		}

		Timestamp timestamp = (Timestamp) val.getFieldMap().get(field_part_year);
		if(timestamp == null)
			return new Text(key);
				
		Calendar cal = Calendar.getInstance(CalendarFieldPartitioner.UTC);
	    cal.setTimeInMillis(timestamp.getTime());

		if(field_part_year != null){
			key = key + "-" + cal.get(Calendar.YEAR);
		}

		if(field_part_month != null){
			key = key + "-" + (cal.get(Calendar.MONTH) + 1);
		}
		
		if(field_part_day != null){
			key = key + "-" + cal.get(Calendar.DAY_OF_MONTH);
		}
		
		if(field_part_hour != null){
			key = key + "-" + cal.get(Calendar.HOUR_OF_DAY);
		}

		return new Text(key);
	}

	@Override
	protected void cleanup(Context context) throws IOException {
		if (null != lobLoader) {
			lobLoader.close();
		}
	}

}