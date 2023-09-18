package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.structs.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LsmStorage {

    /**
     * 每8k数据为一块
     */
    public static final int MAX_ITEM_CNT_L0 = 8 * 1024;

    private final File dir;

    private final Vin vin;

    private final TableSchema tableSchema;

    /**
     * 数据文件
     */
    private final Map<String, ColumnChannel> columnChannelMap = new HashMap<>();

    private final Map<String, ColumnValue.ColumnType> columnTypeMap = new HashMap<>();

    private final TimeChannel timeChannel;

    private final FileChannel metaChannel;

    private Long latestTime;

    private Row latestRow;

    public LsmStorage(File dbDir, Vin vin, TableSchema tableSchema) {
        String vinStr = new String(vin.getVin(), StandardCharsets.UTF_8);
        this.dir = new File(dbDir.getAbsolutePath(), vinStr);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        this.vin = vin;
        this.tableSchema = tableSchema;
        try {
            this.timeChannel = new TimeChannel(dir);

            File vinFile = new File(dir, vinStr + ".meta");
            metaChannel = new RandomAccessFile(vinFile, "rw").getChannel();
            if (metaChannel.size() == 0) {
                latestTime = 0L;
            } else {
                latestTime = metaChannel.map(FileChannel.MapMode.READ_ONLY, 0, 8).getLong();
            }

            for (TableSchema.Column column : tableSchema.getColumnList()) {
                columnTypeMap.put(column.columnName, column.columnType);
                if (column.columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)) {
                    columnChannelMap.put(column.columnName, new IntChannel(dir, column));
                } else if (column.columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {
                    columnChannelMap.put(column.columnName, new DoubleChannel(dir, column));
                } else if (column.columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_STRING)) {
                    columnChannelMap.put(column.columnName, new StringChannel(dir, column));
                } else {
                    throw new IllegalStateException("无效列类型");
                }
            }
            getLatestRow();
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException("LsmStorage初始化失败");
        }
    }

    private Row deepClone(Row row) {
        Map<String, ColumnValue> columns = row.getColumns();
        Map<String, ColumnValue> columnsClone = new HashMap<>(columns.size());
        columns.forEach((k, v) -> {
            if (v.getColumnType() == ColumnValue.ColumnType.COLUMN_TYPE_STRING) {
                ByteBuffer stringValue = v.getStringValue();

                ByteBuffer allocate = ByteBuffer.allocate(stringValue.limit());
                allocate.put(stringValue);
                allocate.flip();

                // 只有这个是可能会变的 其他都是final的
                v = new ColumnValue.StringColumn(allocate);
            }
            columnsClone.put(k, v);
        });
        return new Row(row.getVin(), row.getTimestamp(), columnsClone);
    }

    public void append(Row row) throws IOException {
        if (row.getTimestamp() >= latestTime) {
            latestRow = deepClone(row);
        }
        latestTime = Math.max(row.getTimestamp(), latestTime);
        timeChannel.append(row.getTimestamp());
        row.getColumns().forEach((k, v) -> {
            try {
                columnChannelMap.get(k).append(v);
            } catch (IOException e) {
                e.printStackTrace();
                throw new IllegalStateException(v.getColumnType() + "列插入失败");
            }
        });
    }

    public Row agg(long l, long r, String columnName, Aggregator aggregator, CompareExpression columnFilter) throws IOException {
        if (columnTypeMap.get(columnName).equals(ColumnValue.ColumnType.COLUMN_TYPE_STRING)) {
            throw new IllegalStateException("string类型不支持聚合");
        }

        List<TimeItem> timeRange = timeChannel.range(l, r);
        if (timeRange.isEmpty()) {
            return null;
        }
        ColumnChannel columnChannel = columnChannelMap.get(columnName);
        ColumnValue agg = columnChannel.agg(timeRange, aggregator, columnFilter);
        Map<String, ColumnValue> columnValueMap = new HashMap<>(1);
        columnValueMap.put(columnName, agg);
        return new Row(vin, l, columnValueMap);
    }

    public List<Row> range(long l, long r) throws IOException {
        List<TimeItem> timeRange = timeChannel.range(l, r);
        if (timeRange.isEmpty()) {
            return new ArrayList<>(0);
        }

        ArrayList<Row> rowList = new ArrayList<>(timeRange.size());
        Map<String, List<ColumnValue>> columnValueListMap = new HashMap<>(columnChannelMap.size());
        columnChannelMap.forEach((k, v) -> {
            try {
                List<ColumnItem<ColumnValue>> columnItemList = v.range(timeRange);
                List<ColumnValue> columnValueList = new ArrayList<>(columnItemList.size());
                for (ColumnItem<ColumnValue> columnItem : columnItemList) {
                    columnValueList.add(columnItem.getItem());
                }
                columnValueListMap.put(k, columnValueList);
            } catch (IOException e) {
                e.printStackTrace();
                throw new IllegalStateException(k + "列查询失败");
            }
        });
        for (int i = 0; i < timeRange.size(); i++) {
            Map<String, ColumnValue> columnValueMap = new HashMap<>(columnValueListMap.size());
            int finalI = i;
            columnValueListMap.forEach((k, v) -> columnValueMap.put(k, v.get(finalI)));
            rowList.add(new Row(vin, timeRange.get(i).getTime(), columnValueMap));
        }
        return rowList;
    }

    public void shutdown() {
        try {
            ByteBuffer allocate = ByteBuffer.allocate(8);
            allocate.putLong(latestTime);
            allocate.flip();
            metaChannel.write(allocate, 0);
            metaChannel.close();

            timeChannel.shutdown();
            for (ColumnChannel columnChannel : columnChannelMap.values()) {
                columnChannel.shutdown();
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
            throw new IllegalStateException("LsmStorage shutdown failed.");
        }
    }

    public Vin getVin() {
        return vin;
    }

    public Long getLatestTime() {
        return latestTime;
    }

    public Row getLatestRow() throws IOException {
        if (latestRow != null) {
            return latestRow;
        }
        List<Row> rowList = range(latestTime, latestTime + 1);
        if (rowList != null && rowList.size() > 0) {
            latestRow = rowList.get(0);
        }
        return latestRow;
    }

    public long getTimeIndexFileSize() {
        return timeChannel.getIndexFileSize();
    }
}
