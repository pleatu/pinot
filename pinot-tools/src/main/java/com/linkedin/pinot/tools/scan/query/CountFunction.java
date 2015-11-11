/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools.scan.query;

import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import java.util.ArrayList;
import java.util.List;


public class CountFunction extends AggregationFunc {
  CountFunction(ResultTable rows, String column) {
    super(rows, column);
  }

  @Override
  public ResultTable run() {
    Long count = 0l;
    int colIndex = _rows.getColumnIndex(_column);
    for (ResultTable.Row row : _rows) {
      count += new Long((row.get(colIndex)).toString());
    }

    List<String> columns = new ArrayList<>();
    columns.add(_column);

    ResultTable resultTable = new ResultTable(columns, 1);
    resultTable.add(0, count);

    return resultTable;
  }
}
