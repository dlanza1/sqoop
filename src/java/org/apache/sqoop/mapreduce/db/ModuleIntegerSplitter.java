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
package org.apache.sqoop.mapreduce.db;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import com.cloudera.sqoop.config.ConfigurationHelper;
import com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat;

/**
 * Implement DBSplitter over integer values.
 */
public class ModuleIntegerSplitter extends IntegerSplitter  {
  public static final Log LOG =
      LogFactory.getLog(ModuleIntegerSplitter.class.getName());

    public List<InputSplit> split(Configuration conf, int module,
        String colName) throws SQLException {
    
      String lowClausePrefix = "mod(" + colName + ", " + module + ") >= ";
      String highClausePrefix = "mod(" + colName + ", " + module + ") < ";
      String lastClausePrefix = "mod(" + colName + ", " + module + ") <= ";

      int numSplits = ConfigurationHelper.getConfNumMaps(conf);
      if (numSplits < 1) {
        numSplits = 1;
      }

      // Get all the split points together.
      List<Long> splitPoints = split(numSplits, 0, module);
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Splits: [%,28d to %,28d] into %d parts",
            0, module, numSplits));
        for (int i = 0; i < splitPoints.size(); i++) {
          LOG.debug(String.format("%,28d", splitPoints.get(i)));
        }
      }
      List<InputSplit> splits = new ArrayList<InputSplit>();

      // Turn the split points into a set of intervals.
      long start = splitPoints.get(0);
      for (int i = 1; i < splitPoints.size(); i++) {
        long end = splitPoints.get(i);

        if (i == splitPoints.size() - 1) {
          // This is the last one; use a closed interval.
          splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
              lowClausePrefix + Long.toString(start),
              lastClausePrefix + Long.toString(end)));
        } else {
          // Normal open-interval case.
          splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
              lowClausePrefix + Long.toString(start),
              highClausePrefix + Long.toString(end)));
        }

        start = end;
      }

      return splits;
    }
    
}
