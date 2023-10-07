package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.structs.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class LsmStorage {

    public static final int IO_MODE = 2;

    /**
     * 每8k数据为一块
     */
    public static final int MAX_ITEM_CNT_L0 = 8 * 1024;

    public static final int OUTPUT_BUFFER_SIZE = 8 * 1024;

    private static ThreadPoolExecutor COLUMN_EXECUTOR;

    private static int COLUMN_EXECUTOR_FLAG = 0;

    private synchronized static void initColumnExecutor(int columnCnt) {
        if (COLUMN_EXECUTOR_FLAG != 0) {
            return;
        }

        int threadCnt = Math.max(columnCnt, Runtime.getRuntime().availableProcessors() * 2);

        COLUMN_EXECUTOR = new ThreadPoolExecutor(threadCnt, threadCnt, 1L, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(MAX_ITEM_CNT_L0), new ThreadPoolExecutor.CallerRunsPolicy());

        COLUMN_EXECUTOR_FLAG = 1;
    }

    private synchronized static void shutdownColumnExecutor() {
        if (COLUMN_EXECUTOR_FLAG != 1) {
            return;
        }
        COLUMN_EXECUTOR.shutdown();

        COLUMN_EXECUTOR_FLAG = 0;
    }

    private final File dir;

    private final Vin vin;

    private final TableSchema tableSchema;

    /**
     * 数据文件
     */
    private final Map<String, ColumnChannel> columnChannelMap = new HashMap<>();

    private final Map<String, ColumnValue.ColumnType> columnTypeMap = new HashMap<>();

    private final TimeChannel timeChannel;

    private final DataChannel columnIndexChannel;

    private int columnIndexItemSize;

    private final FileChannel metaChannel;

    private Long latestTime;

    /**
     * todo 软引用
     */
    private Row latestRow;

    private Map<String, Map<Long, ColumnIndexItem>> columnIndexMap = new HashMap<>();

    public LsmStorage(File dbDir, Vin vin, TableSchema tableSchema) {
//        initColumnExecutor(tableSchema.getColumnList().size());
//        initColumnFlusher(tableSchema.getColumnList().size());

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
                    columnIndexItemSize += IntChannel.IDX_SIZE;
                    column.indexSize = IntChannel.IDX_SIZE;
                } else if (column.columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {
                    columnChannelMap.put(column.columnName, new DoubleChannel(dir, column));
                    columnIndexItemSize += DoubleChannel.IDX_SIZE;
                    column.indexSize = DoubleChannel.IDX_SIZE;
                } else if (column.columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_STRING)) {
                    columnChannelMap.put(column.columnName, new StringChannel(dir, column));
                    columnIndexItemSize += StringChannel.IDX_SIZE;
                    column.indexSize = StringChannel.IDX_SIZE;
                } else {
                    throw new IllegalStateException("无效列类型");
                }
            }

            File columnIndexFile = new File(dir, "column.idx");
            if (!columnIndexFile.exists()) {
                columnIndexFile.createNewFile();
            }
            columnIndexChannel = new DataChannel(columnIndexFile, LsmStorage.IO_MODE, 8, LsmStorage.OUTPUT_BUFFER_SIZE);

            getLatestRow();

            loadAllColumnIndexForInit();
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
        boolean idx = timeChannel.checkAndIndex();

//        CountDownLatch countDownLatch = new CountDownLatch(columnChannelMap.size());
        // 按schema顺序
        tableSchema.getColumnList().forEach(column -> {
//            COLUMN_EXECUTOR.execute(() -> {
            try {
                columnChannelMap.get(column.columnName).append(row.getColumns().get(column.columnName), columnIndexChannel, columnIndexMap.get(column.columnName));
                if (idx) {
                    if (column.columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)) {

                    }
                    if (column.columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {

                    }
                    if (column.columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_STRING)) {

                    }
                }
//                    countDownLatch.countDown();
            } catch (IOException e) {
                e.printStackTrace();
                throw new IllegalStateException(column.columnType + "列插入失败");
            }
//            });
        });
//        try {
//            countDownLatch.await();
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }

//        DIRTY_LSM_STORAGE_SET.put(this, PRESENT);
    }

    private void loadAllColumnIndexForInit() throws IOException {
        Set<String> columnNameSet = columnChannelMap.keySet();
        List<TimeIndexItem> timeIndexItemList = timeChannel.loadAllIndexForInit();
        List<TimeItem> timeItemList = new ArrayList<>();
        int indexItemCount = timeIndexItemList.size();
        for (int i = 0; i < indexItemCount; i++) {
            timeItemList.add(new TimeItem(0, (long) LsmStorage.MAX_ITEM_CNT_L0 * i));
        }
        for (String columnName : columnNameSet) {
            columnIndexMap.put(columnName, loadColumnIndex(timeItemList, columnName));
        }
    }

    private Map<Long, ColumnIndexItem> loadColumnIndex(List<TimeItem> timeItemList, String columnName) throws IOException {
        columnIndexChannel.flush();
        Map<Long, ColumnIndexItem> columnIndexItemMap = new HashMap<>();
        Set<Long> batchNumSet = timeItemList.stream().map(TimeItem::getBatchNum).collect(Collectors.toSet());
        List<Long> batchNumList = batchNumSet.stream().sorted().collect(Collectors.toList());
        long pos = batchNumList.get(0) * columnIndexItemSize;
        int size = (int) ((batchNumList.get(batchNumList.size() - 1) - batchNumList.get(0) + 1) * columnIndexItemSize);
        if (pos >= columnIndexChannel.channelSize()) {
            // 这是半包批次，没有写入列索引文件，靠各列自行恢复
            return columnIndexItemMap;
        }
        ByteBuffer byteBuffer = columnIndexChannel.read(pos, size);
        int begin = 0;
        for (TableSchema.Column column : tableSchema.getColumnList()) {
            if (column.columnName.equals(columnName)) {
                break;
            }
            begin += column.indexSize;
        }
        int i = 0;
        while (byteBuffer.hasRemaining() && i < batchNumList.size()) {
            int indexBegin = i * columnIndexItemSize + begin;
            if (indexBegin >= byteBuffer.limit()) {
                i++;
                continue;
            }
            byteBuffer.position(indexBegin);
            ColumnChannel columnChannel = columnChannelMap.get(columnName);
            ColumnIndexItem columnIndexItem = columnChannel.readColumnIndexItem(byteBuffer);
            columnIndexItemMap.put(batchNumList.get(i), columnIndexItem);
            i++;
        }
        return columnIndexItemMap;
    }

    public Row agg(long l, long r, String columnName, Aggregator aggregator, CompareExpression columnFilter) throws IOException {
        if (columnTypeMap.get(columnName).equals(ColumnValue.ColumnType.COLUMN_TYPE_STRING)) {
            throw new IllegalStateException("string类型不支持聚合");
        }

        List<TimeItem> timeRange = timeChannel.agg(l, r);
        if (timeRange.isEmpty()) {
            return null;
        }
        List<TimeItem> batch = timeRange.stream().filter(timeItem -> timeItem.getTime() == 0).collect(Collectors.toList());
        ColumnChannel columnChannel = columnChannelMap.get(columnName);
        ColumnValue agg = columnChannel.agg(batch, timeRange, aggregator, columnFilter, loadColumnIndex(timeRange, columnName));
        Map<String, ColumnValue> columnValueMap = new HashMap<>(1);
        columnValueMap.put(columnName, agg);
        return new Row(vin, l, columnValueMap);
    }

    public ArrayList<Row> range(long l, long r, Set<String> requestedColumnSet) throws IOException {
        List<TimeItem> timeRange = timeChannel.range(l, r);
        if (timeRange.isEmpty()) {
            return new ArrayList<>(0);
        }

        ArrayList<Row> rowList = new ArrayList<>(timeRange.size());
        Map<String, List<ColumnValue>> columnValueListMap = new ConcurrentHashMap<>(columnChannelMap.size());
//        CountDownLatch countDownLatch = new CountDownLatch(columnChannelMap.size());
        for (String columnName : requestedColumnSet) {
            ColumnChannel columnChannel = columnChannelMap.get(columnName);
//            COLUMN_EXECUTOR.execute(() -> {
            try {
                List<ColumnItem<ColumnValue>> columnItemList = columnChannel.range(timeRange, loadColumnIndex(timeRange, columnName));
                List<ColumnValue> columnValueList = new ArrayList<>(columnItemList.size());
                for (ColumnItem<ColumnValue> columnItem : columnItemList) {
                    columnValueList.add(columnItem.getItem());
                }
                columnValueListMap.put(columnName, columnValueList);
//                countDownLatch.countDown();
            } catch (IOException e) {
                e.printStackTrace();
                throw new IllegalStateException(columnName + "列查询失败");
            }
//            });
        }
//        try {
//            countDownLatch.await();
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
        for (int i = 0; i < timeRange.size(); i++) {
            Map<String, ColumnValue> columnValueMap = new HashMap<>(columnValueListMap.size());
            int finalI = i;
            columnValueListMap.forEach((k, v) -> columnValueMap.put(k, v.get(finalI)));
            rowList.add(new Row(vin, timeRange.get(i).getTime(), columnValueMap));
        }
        return rowList;
    }

    public void flush() throws IOException {
        timeChannel.flush();
        columnIndexChannel.flush();

        for (ColumnChannel columnChannel : columnChannelMap.values()) {
            columnChannel.flush();
        }
    }

    public void shutdown() {
        try {
            shutdownColumnExecutor();

            ByteBuffer allocate = ByteBuffer.allocate(8);
            allocate.putLong(latestTime);
            allocate.flip();
            metaChannel.write(allocate, 0);
            metaChannel.close();

            columnIndexChannel.flush();
            columnIndexChannel.close();

            timeChannel.shutdown();
            for (ColumnChannel columnChannel : columnChannelMap.values()) {
                columnChannel.shutdown();
            }

//            COLUMN_EXECUTOR.shutdown();
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
        List<Row> rowList = range(latestTime, latestTime + 1, columnChannelMap.keySet());
        if (rowList != null && rowList.size() > 0) {
            latestRow = rowList.get(0);
        }
        return latestRow;
    }

    public long getTimeIndexFileSize() {
        return timeChannel.getIndexFileSize();
    }

    public long getColumnIndexFileSize() {
        try {
            return columnIndexChannel.channelSize();
        } catch (IOException e) {
            System.out.println("getColumnIndexFileSize failed.");
            return 0;
        }
    }
}
