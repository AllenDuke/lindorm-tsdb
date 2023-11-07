//
// A simple evaluation program example helping you to understand how the
// evaluation program calls the protocols you will implement.
// Formal evaluation program is much more complex than this.
//

/*
 * Copyright Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.alibaba.lindorm.contest.TSDBEngine;
import com.alibaba.lindorm.contest.TSDBEngineImpl;
import com.alibaba.lindorm.contest.lsm.LsmStorage;
import com.alibaba.lindorm.contest.structs.*;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TestDownSample {
    public static void main(String[] args) {
        File dataDir = new File("data_dir");

        if (dataDir.isFile()) {
            throw new IllegalStateException("Clean the directory before we start the demo");
        }

        TSDBEngine tsdbEngineSample = new TSDBEngineImpl(dataDir);
        String str = "LSVNV2182E0541854";

        try {
            tsdbEngineSample.connect();

            ArrayList<Vin> vinList = new ArrayList<>();
            vinList.add(new Vin(str.getBytes(StandardCharsets.UTF_8)));
            Set<String> requestedColumns = new HashSet<>(Arrays.asList("col1", "col2", "col3"));
            ArrayList<Row> resultSet = tsdbEngineSample.executeLatestQuery(new LatestQueryRequest("test", vinList, requestedColumns));
            showResult(resultSet);

            long begin = System.currentTimeMillis();

            resultSet = tsdbEngineSample.executeDownsampleQuery(new TimeRangeDownsampleRequest("test",
                    new Vin(str.getBytes(StandardCharsets.UTF_8)), "col1", TestMyDb.UTC + TestMyDb.ITEM_CNT-100,
                    TestMyDb.UTC + TestMyDb.ITEM_CNT+3600, Aggregator.AVG, 300,
                    new CompareExpression(new ColumnValue.IntegerColumn(TestMyDb.ITEM_CNT-1), CompareExpression.CompareOp.EQUAL)));
            showResult(resultSet);

            System.out.println((System.currentTimeMillis() - begin));

            tsdbEngineSample.shutdown();
        } catch (Throwable e) {
            System.out.println(e.getMessage());
        }
    }

    public static void showResult(ArrayList<Row> resultSet) {
        for (Row result : resultSet)
            System.out.println(result);
        System.out.println("-------next query-------");
    }
}