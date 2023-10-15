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
import com.alibaba.lindorm.contest.TSDBEngineImpl;
import com.alibaba.lindorm.contest.TSDBEngine;
import com.alibaba.lindorm.contest.lsm.LsmStorage;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.LatestQueryRequest;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Schema;
import com.alibaba.lindorm.contest.structs.TimeRangeQueryRequest;
import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.structs.WriteRequest;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class TestMyDb {

    public static final long UTC = Date.UTC(1970, Calendar.FEBRUARY, 1, 0, 0, 0);

    public static final int ITEM_CNT = 40000000;

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

        try {
            // Stage1: write
            tsdbEngineSample.connect();

            Map<String, ColumnValue> columns = new HashMap<>();
            int bufferSize = 100;
            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
            for (int i = 0; i < bufferSize; i++) {
                buffer.put((byte) 'h');
            }
            buffer.flip();
            columns.put("col1", new ColumnValue.IntegerColumn(123));
            columns.put("col2", new ColumnValue.DoubleFloatColumn(1.23));
            columns.put("col3", new ColumnValue.StringColumn(buffer));


            Map<String, ColumnValue.ColumnType> cols = new HashMap<>();
            cols.put("col1", ColumnValue.ColumnType.COLUMN_TYPE_INTEGER);
            cols.put("col2", ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
            cols.put("col3", ColumnValue.ColumnType.COLUMN_TYPE_STRING);
            Schema schema = new Schema(cols);

            ArrayList<Vin> vinList = new ArrayList<>();
            vinList.add(new Vin(str.getBytes(StandardCharsets.UTF_8)));
            Set<String> requestedColumns = new HashSet<>(Arrays.asList("col1", "col2", "col3"));

            tsdbEngineSample.createTable("test", schema);

            long begin = System.currentTimeMillis();

            ArrayList<Row> rowList = new ArrayList<>();
            Random random=new Random();
            for (int i = 0; i < ITEM_CNT; i++) {
                rowList.clear();

                columns = new HashMap<>();
                columns.put("col1", new ColumnValue.IntegerColumn(i));
                columns.put("col2", new ColumnValue.DoubleFloatColumn(0.123456+i%10));
                columns.put("col3", new ColumnValue.StringColumn(buffer));
                rowList.add(new Row(new Vin(str.getBytes(StandardCharsets.UTF_8)), UTC + i, columns));

                tsdbEngineSample.write(new WriteRequest("test", rowList));

                if (i != 0 && i % 1000_0000 == 0) {
                    ArrayList<Row> resultSet = tsdbEngineSample.executeTimeRangeQuery(new TimeRangeQueryRequest("test",
                            new Vin(str.getBytes(StandardCharsets.UTF_8)), requestedColumns, UTC + i, UTC + i + 1));
                    showResult(resultSet);
                }
            }

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

            System.out.println((System.currentTimeMillis() - begin));

            // Stage2: read
            tsdbEngineSample.connect();

            ArrayList<Row> resultSet = tsdbEngineSample.executeLatestQuery(new LatestQueryRequest("test", vinList, requestedColumns));
            showResult(resultSet);

            tsdbEngineSample.shutdown();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void showResult(ArrayList<Row> resultSet) {
        for (Row result : resultSet)
            System.out.println(result);
        System.out.println("-------next query-------");
    }
}