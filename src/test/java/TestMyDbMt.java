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

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.TSDBEngine;
import com.alibaba.lindorm.contest.TSDBEngineImpl;
import com.alibaba.lindorm.contest.structs.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TestMyDbMt {
    public static void main(String[] args) {

        File dataDir = new File("data_dir");

        if (dataDir.isFile()) {
            throw new IllegalStateException("Clean the directory before we start the demo");
        }

        CommonUtils.cleanDir(dataDir, true);

        boolean ret = dataDir.mkdirs();
        if (!ret) {
            throw new IllegalStateException("Cannot create the temp data directory: " + dataDir);
        }

        TSDBEngine tsdbEngineSample = new TSDBEngineImpl(dataDir);
        String str = "12345678912345678";
        String str1 = "123456789123456789";

        try {
            // Stage1: write
            tsdbEngineSample.connect();

            Map<String, ColumnValue> columns = new HashMap<>();
            ByteBuffer buffer = ByteBuffer.allocate(3);
            buffer.put((byte) 70);
            buffer.put((byte) 71);
            buffer.put((byte) 72);
            buffer.flip();
            columns.put("col1", new ColumnValue.IntegerColumn(123));
            columns.put("col2", new ColumnValue.DoubleFloatColumn(1.23));
            columns.put("col3", new ColumnValue.StringColumn(buffer));


            Map<String, ColumnValue.ColumnType> cols = new HashMap<>();
            cols.put("col1", ColumnValue.ColumnType.COLUMN_TYPE_INTEGER);
            cols.put("col2", ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
            cols.put("col3", ColumnValue.ColumnType.COLUMN_TYPE_STRING);
            Schema schema = new Schema(cols);

            tsdbEngineSample.createTable("test", schema);

            ThreadPoolExecutor wExecutor = new ThreadPoolExecutor(3, 3, 1, TimeUnit.MINUTES, new LinkedBlockingDeque<>());
            int rowCnt = 100000;
            for (int i = 0; i < rowCnt; i++) {
                int finalI = i;
                wExecutor.submit(() -> {
                    ArrayList<Row> rowList = new ArrayList<>();
                    rowList.add(new Row(new Vin(str.getBytes(StandardCharsets.UTF_8)), finalI, columns));
                    rowList.add(new Row(new Vin(str1.getBytes(StandardCharsets.UTF_8)), finalI, columns));
                    try {
                        tsdbEngineSample.upsert(new WriteRequest("test", rowList));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
            ThreadPoolExecutor rExecutor = new ThreadPoolExecutor(3, 3, 1, TimeUnit.MINUTES, new LinkedBlockingDeque<>());
            for (int i = 0; i < rowCnt; i++) {
                rExecutor.submit(() -> {
                    ArrayList<Vin> vinList = new ArrayList<>();
                    vinList.add(new Vin(str.getBytes(StandardCharsets.UTF_8)));
                    vinList.add(new Vin(str1.getBytes(StandardCharsets.UTF_8)));
                    Set<String> requestedColumns = new HashSet<>(Arrays.asList("col1", "col2", "col3"));
                    try {
                        ArrayList<Row> resultSet = tsdbEngineSample.executeLatestQuery(new LatestQueryRequest("test", vinList, requestedColumns));
                        showResult(resultSet);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
            wExecutor.shutdown();
            wExecutor.awaitTermination(3, TimeUnit.MINUTES);
            rExecutor.shutdown();
            rExecutor.awaitTermination(3, TimeUnit.MINUTES);

//            rowList.clear();
//            columns.put("col3", new ColumnValue.StringColumn(ByteBuffer.allocate((int) (AbPage.PAGE_SIZE + 1))));
//            Row bigRow = new Row(new Vin(str.getBytes(StandardCharsets.UTF_8)), 12345, columns);
//            rowList.add(bigRow);
//            tsdbEngineSample.upsert(new WriteRequest("test", rowList));
//
//            rowList.clear();
//            bigRow = new Row(new Vin(str.getBytes(StandardCharsets.UTF_8)), 666, columns);
//            rowList.add(bigRow);
//            tsdbEngineSample.upsert(new WriteRequest("test", rowList));
//
//            rowList.clear();
//            columns.put("col3", new ColumnValue.StringColumn(ByteBuffer.allocate(10)));
//            Row smallRow = new Row(new Vin(str.getBytes(StandardCharsets.UTF_8)), 12345, columns);
//            rowList.add(smallRow);
//            tsdbEngineSample.upsert(new WriteRequest("test", rowList));
//
//            rowList.clear();
//            columns.put("col3", new ColumnValue.StringColumn(ByteBuffer.allocate((int) (AbPage.PAGE_SIZE * 2))));
//            bigRow = new Row(new Vin(str.getBytes(StandardCharsets.UTF_8)), 666, columns);
//            rowList.add(bigRow);
//            tsdbEngineSample.upsert(new WriteRequest("test", rowList));

            tsdbEngineSample.shutdown();

            // Stage2: read
            tsdbEngineSample.connect();

            ArrayList<Vin> vinList = new ArrayList<>();
            vinList.add(new Vin(str.getBytes(StandardCharsets.UTF_8)));
            Set<String> requestedColumns = new HashSet<>(Arrays.asList("col1", "col2", "col3"));
            ArrayList<Row> resultSet = tsdbEngineSample.executeLatestQuery(new LatestQueryRequest("test", vinList, requestedColumns));
            showResult(resultSet);

            resultSet = tsdbEngineSample.executeTimeRangeQuery(new TimeRangeQueryRequest("test",
                    new Vin(str.getBytes(StandardCharsets.UTF_8)), requestedColumns, 0, 100));
            showResult(resultSet);

            tsdbEngineSample.shutdown();
        } catch (
                IOException | InterruptedException e) {
            System.out.println(e.getMessage());
        }

    }

    public static void showResult(ArrayList<Row> resultSet) {
        for (Row result : resultSet)
            System.out.println(result);
        System.out.println("-------next query-------");
    }
}