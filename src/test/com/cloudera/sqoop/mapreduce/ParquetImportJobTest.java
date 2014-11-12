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

package com.cloudera.sqoop.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.sqoop.Sqoop;

import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;

import org.apache.sqoop.tool.ImportTool;

/**
 * Test aspects of the DataDrivenImportJob class' failure reporting.
 *
 * These tests have strange error checking because they have different correct
 * exit conditions when run in "debug mode" when run by ('ant test') and when
 * run within eclipse.  Debug mode is entered by setting system property
 * SQOOP_RETHROW_PROPERTY = "sqoop.throwOnError".
 */
public class ParquetImportJobTest extends ImportJobTestCase {
  public void testFailedImportDueToIOException() throws IOException {
	  
	//Delete output folders
	super.guaranteeCleanWarehouse();
	  
    //Create a table to attempt to import.
	String[] colNames = {"VARIABLE_ID","UTC_STAMP",							"VALUE"};
	String[] colTypes = {"INT",        "TIMESTAMP",							"DOUBLE"};
	String[] vals =     {"1",          "'2003-09-04 16:42:58.123456781'",	"123.3",
						 "2",          "'2003-09-05 17:42:58.135791231'",	"13.3",
						 "3",          "'2003-09-05 17:42:58.135791231'",	"13.3",
						 "4",          "'2003-09-05 17:42:58.135791231'",	"13.3",
						 "5",          "'2003-09-05 17:42:58.135791231'",	"13.3",
						 "6",          "'2003-09-05 17:42:58.135791231'",	"13.3",
						 "7",          "'2003-09-05 17:42:58.135791231'",	"13.3",
						 "8",          "'2003-09-05 17:42:58.135791231'",	"13.3",
						 "9",          "'2003-09-05 17:42:58.135791231'",	"13.3",
						 "10",         "'2003-09-05 17:42:58.135791231'",	"13.3",
						 "11",         "'2003-09-05 17:42:58.135791231'",	"13.3",
						 "12",         "'2003-09-05 17:42:58.135791231'",	"13.3",
						 "3",          "'2003-09-05 17:42:58.435134541'",	null};
	
	setCurTableName("LHCLOG_DATA_NUMERIC");
	createTableWithColTypesAndNames(colNames, colTypes, vals);

    LogFactory.getLog(getClass()).info(
            " getWarehouseDir() " + getWarehouseDir());

    // Delete output dir if exist
    Path outputPath = new Path(new Path(getWarehouseDir()), getTableName());

    ArrayList<String> args = new ArrayList<String>();
    CommonArgs.addHadoopFlags(args);
    args.add("--table");
    args.add(getTableName());
    args.add("--split-by");
    args.add("UTC_STAMP");
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(getConnectString());
    args.add("--as-parquetfile");
    args.add("--reduce-phase");
    args.add("--num-mappers");
    args.add("2");
    args.add("--num-reducers");
    args.add("3");
    String[] argv = args.toArray(new String[0]);

    Sqoop importer = new Sqoop(new ImportTool());
    try {
      assertEquals(Sqoop.runSqoop(importer, argv), 0);
    } catch (Exception e) {
      // In debug mode, IOException is wrapped in RuntimeException.
      LOG.info("Got exceptional return (expected: ok). msg is: " + e);
    }
    System.out.println(outputPath);
  }

  protected void guaranteeCleanWarehouse() {}
}
