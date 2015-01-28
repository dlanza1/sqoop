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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.sqoop.lib.SqoopRecord;

import com.cloudera.sqoop.lib.LargeObjectLoader;

/**
 * Imports records by writing them to a Parquet File.
 */
public class ParquetPartitionedImportMapper extends
		AutoProgressMapper<LongWritable, SqoopRecord, LongWritable, SqoopRecord> {

	private static int module;
	private LongWritable output_key;
	private static String module_field;
	
	private LargeObjectLoader lobLoader = null;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		
		module = conf.getInt("partitioning.module", Integer.MAX_VALUE);
		module_field = conf.get("partitioning.module.field");
		
		output_key = new LongWritable();
		
		lobLoader = new LargeObjectLoader(conf, new Path(conf.get("sqoop.kite.lob.extern.dir", "/tmp/sqoop-parquet-" + context.getTaskAttemptID())));
	}
	
	protected void map(
			LongWritable key,
			SqoopRecord val,
			Context context)
			throws IOException, InterruptedException {
		
		// Loading of LOBs was delayed until we have a Context.
		try {
			val.loadLargeObjects(lobLoader);
			
			long value = (long) val.getFieldMap().get(module_field);
			output_key.set(value % module);
			
			context.write(output_key, val);
		} catch (SQLException e) {
			e.printStackTrace();

			throw new IOException(e);
		}
		
	}


	@Override
	protected void cleanup(Context context) throws IOException {
		if (null != lobLoader) {
			lobLoader.close();
		}
	}

}