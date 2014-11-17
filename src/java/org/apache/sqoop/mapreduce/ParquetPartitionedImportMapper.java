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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.sqoop.lib.SqoopRecord;

/**
 * Imports records by writing them to a Parquet File.
 */
public class ParquetPartitionedImportMapper extends
		AutoProgressMapper<LongWritable, SqoopRecord, IntWritable, SqoopRecord> {

	private static int module;
	private IntWritable output_key;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		module = context.getConfiguration().getInt("partition.module", Integer.MAX_VALUE);
		
		output_key = new IntWritable();
	}
	
	protected void map(
			LongWritable key,
			SqoopRecord val,
			Context context)
			throws IOException, InterruptedException {
		
		int value = (int) val.getFieldMap().get("VARIABLE_ID");
		
		output_key.set(value % module);
		
		context.write(output_key, val);
	}

}