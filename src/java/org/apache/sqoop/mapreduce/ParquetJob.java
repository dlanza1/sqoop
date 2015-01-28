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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.kitesdk.data.CompressionType;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Formats;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.PartitionStrategy.Builder;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.SchemaValidationUtil;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Helper class for setting up a Parquet MapReduce job.
 */
public final class ParquetJob {

  public static final Log LOG = LogFactory.getLog(ParquetJob.class.getName());

  private ParquetJob() {
  }

  private static final String CONF_AVRO_SCHEMA = "parquetjob.avro.schema";
  static final String CONF_OUTPUT_CODEC = "parquetjob.output.codec";

  public static Schema getAvroSchema(Configuration conf) {
    return new Schema.Parser().parse(conf.get(CONF_AVRO_SCHEMA));
  }

  public static CompressionType getCompressionType(Configuration conf) {
    CompressionType defaults = Formats.PARQUET.getDefaultCompressionType();
    String codec = conf.get(CONF_OUTPUT_CODEC, defaults.getName());
    try {
      return CompressionType.forName(codec);
    } catch (IllegalArgumentException ex) {
      LOG.warn(String.format(
          "Unsupported compression type '%s'. Fallback to '%s'.",
          codec, defaults));
    }
    return defaults;
  }

  /**
   * Configure the import job. The import process will use a Kite dataset to
   * write data records into Parquet format internally. The input key class is
   * {@link org.apache.sqoop.lib.SqoopRecord}. The output key is
   * {@link org.apache.avro.generic.GenericRecord}.
   */
  public static void configureImportJob(Configuration conf, Schema schema,
      String uri, boolean doAppend) throws IOException {
    Dataset dataset;
    if (doAppend) {
      try {
        dataset = Datasets.load(uri);
      } catch (DatasetNotFoundException ex) {
        dataset = createDataset(schema, getCompressionType(conf), uri, conf);
      }
      Schema writtenWith = dataset.getDescriptor().getSchema();
      if (!SchemaValidationUtil.canRead(writtenWith, schema)) {
        throw new IOException(
            String.format("Expected schema: %s%nActual schema: %s",
                writtenWith, schema));
      }
    } else {
      dataset = createDataset(schema, getCompressionType(conf), uri, conf);
    }
    
    conf.set(CONF_AVRO_SCHEMA, schema.toString());

    DatasetKeyOutputFormat.configure(conf).writeTo(dataset);
  }

  private static Dataset createDataset(Schema schema,
      CompressionType compressionType, String uri, Configuration conf) {

	org.kitesdk.data.DatasetDescriptor.Builder descriptor = new DatasetDescriptor.Builder()
      .schema(schema)
      .format(Formats.PARQUET)
      .compressionType(compressionType)
      .property("parquet.file_per_block", conf.getBoolean("parquet.file_per_block", true)+"");
	  
	int kite_cahe_size = conf.getInt("kite.writer.cache-size", -1);
	if(kite_cahe_size != -1)
		descriptor.property("kite.writer.cache-size", kite_cahe_size + "");
	
	PartitionStrategy partStrategy = getPartitionStrategy(conf);
	if(partStrategy != null)
		descriptor.partitionStrategy(partStrategy);
    
    return Datasets.create(uri, descriptor.build(), GenericRecord.class);
  }

  private static PartitionStrategy getPartitionStrategy(Configuration conf) {
	Builder partBuilder = new PartitionStrategy.Builder();
	
	int module = conf.getInt("partitioning.module", -1);
	String field_part_module = conf.get("partitioning.module.field");
	if(module != -1 && field_part_module != null){
		partBuilder.module(field_part_module, module);
	}

	String field_part_year = conf.get("partitioning.year.field");
	if(field_part_year != null){
		partBuilder.year(field_part_year, field_part_year + "_part_year");
	}

	String field_part_month = conf.get("partitioning.month.field");
	if(field_part_month != null){
		partBuilder.month(field_part_month, field_part_month + "_part_month");
	}
	
	String field_part_day = conf.get("partitioning.day.field");
	if(field_part_day != null){
		partBuilder.day(field_part_day, field_part_day + "_part_day");
	}
	
	String field_part_hour = conf.get("partitioning.hour.field");
	if(field_part_hour != null){
		partBuilder.hour(field_part_hour, field_part_hour + "_part_hour");
	}
	
	PartitionStrategy partStrategy = partBuilder.build();
	
	return partStrategy.getCardinality() > 1 
			|| partStrategy.getCardinality() == FieldPartitioner.UNKNOWN_CARDINALITY ? 
					partStrategy : null;
  }

}
