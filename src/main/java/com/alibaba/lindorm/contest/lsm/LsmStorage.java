package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.structs.*;
import com.alibaba.lindorm.contest.util.ByteBufferUtil;
import com.alibaba.lindorm.contest.util.RowUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class LsmStorage {

    public static final int IO_MODE = 2;

    /**
     * 每8k数据为一块
     */
    public static final int MAX_ITEM_CNT_L0 = 1 * 1024;

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

    private final DataChannel indexChannel;

    private int indexItemSize;

    private final FileChannel metaChannel;

    private Long latestTime;

    /**
     * todo 软引用
     */
    private Row latestRow;

    private final List<TimeIndexItem> timeIndexItemList = new ArrayList<>();
    private Map<String, Map<Long, ColumnIndexItem>> columnIndexMap = new HashMap<>();

    private int loadedAllColumnIndexForInit = -1;

    private MappedByteBuffer rowBuffer;

//    private List<Row> notCheckRowList = new ArrayList<>(LsmStorage.MAX_ITEM_CNT_L0);

    private FileChannel rowChannel;

    private long checkTime;

    private int batchItemCount;

    private int batchCount;

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
            File vinFile = new File(dir, vinStr + ".meta");
            metaChannel = new RandomAccessFile(vinFile, "rw").getChannel();
            if (metaChannel.size() == 0) {
                latestTime = 0L;
            } else {
                latestTime = metaChannel.map(FileChannel.MapMode.READ_ONLY, 0, 8).getLong();
            }

            File dataFile = new File(dir.getAbsolutePath(), "time.data");
            if (!dataFile.exists()) {
                dataFile.createNewFile();
            }

            DataChannel dataChannel = new DataChannel(dataFile, LsmStorage.IO_MODE, 16, LsmStorage.OUTPUT_BUFFER_SIZE);
            this.timeChannel = new TimeChannel(vinStr.hashCode(), dataChannel, timeIndexItemList);
            indexItemSize += TimeIndexItem.SIZE;

            dataFile = new File(dir.getAbsolutePath(), "column.data");
            if (!dataFile.exists()) {
                dataFile.createNewFile();
            }

            for (TableSchema.Column column : tableSchema.getColumnList()) {
                columnTypeMap.put(column.columnName, column.columnType);
                if (column.columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)) {
                    columnChannelMap.put(column.columnName, new IntChannel(vinStr.hashCode(), column, dataFile, dataChannel));
                    indexItemSize += IntChannel.IDX_SIZE;
                    column.indexSize = IntChannel.IDX_SIZE;
                } else if (column.columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {
                    columnChannelMap.put(column.columnName, new DoubleChannel(vinStr.hashCode(), column, dataFile, dataChannel));
                    indexItemSize += DoubleChannel.IDX_SIZE;
                    column.indexSize = DoubleChannel.IDX_SIZE;
                } else if (column.columnType.equals(ColumnValue.ColumnType.COLUMN_TYPE_STRING)) {
                    columnChannelMap.put(column.columnName, new StringChannel(vinStr.hashCode(), column, dataFile, dataChannel));
                    indexItemSize += StringChannel.IDX_SIZE;
                    column.indexSize = StringChannel.IDX_SIZE;
                } else {
                    throw new IllegalStateException("无效列类型");
                }
            }

            File indexFile = new File(dir, "idx");
            if (!indexFile.exists()) {
                indexFile.createNewFile();
            }
            indexChannel = new DataChannel(indexFile, LsmStorage.IO_MODE, 16, LsmStorage.OUTPUT_BUFFER_SIZE);
            loadAllIndexForInit();

            if (latestTime != 0) {
                getLatestRow();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException("LsmStorage初始化失败");
        }
    }

    private void insert() throws IOException {
        batchCount++;

        rowBuffer.flip();
        List<Row> rowList = RowUtil.toRowList(tableSchema, rowBuffer);
        rowBuffer.clear();

        Map<String, List<ColumnValue>> columnValuesMap = new HashMap<>(columnChannelMap.size());
        int rowSize = rowList.size();
        long[] times = new long[rowSize];
        int i = 0;
        for (Row cur : rowList) {
            times[i++] = cur.getTimestamp();
            for (String columnName : columnChannelMap.keySet()) {
                columnValuesMap.computeIfAbsent(columnName, v -> new ArrayList<>(rowSize)).add(cur.getColumns().get(columnName));
            }
        }
        timeChannel.append(times);
        timeChannel.index();
        checkTime = latestTime;

        // help gc
        rowList = null;

        // 按schema顺序
        tableSchema.getColumnList().forEach(column -> {
            // help gc
            List<ColumnValue> columnValues = columnValuesMap.remove(column.columnName);
            try {
                columnChannelMap.get(column.columnName).append(columnValues, indexChannel, columnIndexMap.get(column.columnName));
            } catch (IOException e) {
                e.printStackTrace();
                throw new IllegalStateException(column.columnType + "列插入失败");
            }
        });
    }

    public void append(Row row) throws IOException {
//        row = deepClone(row);
//        if (row.getTimestamp() >= latestTime) {
//            latestRow = row;
//        }
        latestTime = Math.max(row.getTimestamp(), latestTime);
        if (rowBuffer == null) {
            File rowFile = new File(dir, "row");
            rowChannel = new RandomAccessFile(rowFile, "rw").getChannel();
            rowBuffer = rowChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4 * 1024 * 1024);
        }
        RowUtil.toByteBuffer(tableSchema, row, rowBuffer);
//        notCheckRowList.add(row);
        batchItemCount++;
        if (batchItemCount >= LsmStorage.MAX_ITEM_CNT_L0) {
            batchItemCount = 0;

            insert();
//            notCheckRowList.clear();
        }
    }

    private void loadAllIndexForInit() throws IOException {
        if (loadedAllColumnIndexForInit != -1) {
            return;
        }
        for (TableSchema.Column column : tableSchema.getColumnList()) {
            columnIndexMap.put(column.columnName, new HashMap<>());
        }
        if (latestTime == 0) {
            return;
        }
        ByteBuffer byteBuffer = indexChannel.read(0L, (int) indexChannel.channelSize());
        byteBuffer = ByteBuffer.wrap(ByteBufferUtil.zstdDecode(byteBuffer));
        long batchNum = 0;
        while (byteBuffer.hasRemaining()) {
            TimeIndexItem timeIndexItem = timeChannel.readIndexItem(byteBuffer);
            timeIndexItemList.add(timeIndexItem);
            for (TableSchema.Column column : tableSchema.getColumnList()) {
                Map<Long, ColumnIndexItem> columnIndexItemMap = columnIndexMap.get(column.columnName);
                ColumnChannel columnChannel = columnChannelMap.get(column.columnName);
                ColumnIndexItem columnIndexItem = columnChannel.readColumnIndexItem(byteBuffer);
                columnIndexItemMap.put(batchNum, columnIndexItem);
            }
            batchNum++;
        }

        loadedAllColumnIndexForInit = (int) batchNum;
    }

    public ArrayList<Row> aggDownSample(long lowerBound, long upperBound, long interval, String columnName, Aggregator aggregator, CompareExpression columnFilter) throws IOException {
        List<TimeItem> timeItemList = timeChannel.range(lowerBound, upperBound);
        long l = lowerBound;
        long r = Math.min(l + interval, upperBound);
        List<Map<Long, List<Long>>> split = new ArrayList<>();
        List<Long> lList = new ArrayList<>();
        int i = 0;
        while (l < upperBound) {
            lList.add(l);
            Map<Long, List<Long>> batchTimeItemSetMap = new LinkedHashMap<>();
            for (int j = i; j < timeItemList.size(); j++) {
                TimeItem timeItem = timeItemList.get(j);
                if (timeItem.getTime() >= r) {
                    i = j;
                    break;
                }
                if (timeItem.getTime() >= l) {
                    List<Long> timeItemSet = batchTimeItemSetMap.computeIfAbsent(timeItem.getBatchNum(), v -> new ArrayList<>());
                    timeItemSet.add(timeItem.getItemNum());
                }
            }

            l = r;
            r = Math.min(l + interval, upperBound);
            split.add(batchTimeItemSetMap);
        }

        List<ColumnValue> notcheckList = new ArrayList<>();
        if (rowBuffer != null && rowBuffer.position() > 0 && checkTime < r) {
            // 在行存储的rowBuffer中
            rowBuffer.flip();
            List<Row> notCheckRowList = RowUtil.toRowList(tableSchema, rowBuffer);
            for (Row row : notCheckRowList) {
                if (row.getTimestamp() < l || row.getTimestamp() >= r) {
                    continue;
                }
                notcheckList.add(row.getColumns().get(columnName));
            }
        }

        ArrayList<Row> rowList = new ArrayList<>();

        ColumnChannel columnChannel = columnChannelMap.get(columnName);
        List<ColumnValue> list = columnChannel.aggDownSample(split, aggregator, columnFilter, columnIndexMap.get(columnName), notcheckList);
        i = 0;
        for (ColumnValue columnValue : list) {
            Map<String, ColumnValue> columnValueMap = new HashMap<>(1);
            columnValueMap.put(columnName, columnValue);
            rowList.add(new Row(vin, lList.get(i++), columnValueMap));
        }

        return rowList;
    }

    public Row agg(long l, long r, String columnName, Aggregator aggregator, CompareExpression columnFilter) throws IOException {
        if (columnTypeMap.get(columnName).equals(ColumnValue.ColumnType.COLUMN_TYPE_STRING)) {
            throw new IllegalStateException("string类型不支持聚合");
        }

        if (l > latestTime) {
            return null;
        }

        List<ColumnValue> notcheckList = new ArrayList<>();
        if (rowBuffer != null && rowBuffer.position() > 0 && checkTime < r) {
            // 在行存储的rowBuffer中
            rowBuffer.flip();
            List<Row> notCheckRowList = RowUtil.toRowList(tableSchema, rowBuffer);
            for (Row row : notCheckRowList) {
                if (row.getTimestamp() < l || row.getTimestamp() >= r) {
                    continue;
                }
                notcheckList.add(row.getColumns().get(columnName));
            }
        }

        List<TimeItem> timeRange = timeChannel.agg(l, r);
        List<TimeItem> batch = new ArrayList<>();
        Map<Long, List<Long>> batchTimeItemSetMap = new LinkedHashMap<>();
        for (TimeItem timeItem : timeRange) {
            if (timeItem.getTime() == 0) {
                batch.add(timeItem);
            } else {
                List<Long> timeItemSet = batchTimeItemSetMap.computeIfAbsent(timeItem.getBatchNum(), v -> new ArrayList<>());
                timeItemSet.add(timeItem.getItemNum());
            }
        }
        ColumnChannel columnChannel = columnChannelMap.get(columnName);
        ColumnValue agg = columnChannel.agg(batch, timeRange, batchTimeItemSetMap, aggregator, columnFilter, columnIndexMap.get(columnName), notcheckList);
        Map<String, ColumnValue> columnValueMap = new HashMap<>(1);
        columnValueMap.put(columnName, agg);
        return new Row(vin, l, columnValueMap);
    }

    public ArrayList<Row> range(long l, long r, Set<String> requestedColumnSet) throws IOException {
        if (l > latestTime) {
            return new ArrayList<>(0);
        }

        List<TimeItem> timeRange = timeChannel.range(l, r);
        ArrayList<Row> rowList = new ArrayList<>(timeRange.size());
        Map<String, List<ColumnValue>> columnValueListMap = new ConcurrentHashMap<>(columnChannelMap.size());

        Map<Long, List<Long>> batchTimeItemSetMap = new LinkedHashMap<>();
        for (TimeItem timeItem : timeRange) {
            List<Long> timeItemSet = batchTimeItemSetMap.computeIfAbsent(timeItem.getBatchNum(), v -> new ArrayList<>());
            timeItemSet.add(timeItem.getItemNum());
        }

//        CountDownLatch countDownLatch = new CountDownLatch(columnChannelMap.size());
        for (String columnName : requestedColumnSet) {
            ColumnChannel columnChannel = columnChannelMap.get(columnName);
//            COLUMN_EXECUTOR.execute(() -> {
            try {
                List<ColumnItem<ColumnValue>> columnItemList = columnChannel.range(timeRange, batchTimeItemSetMap, columnIndexMap.get(columnName));
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
            columnValueListMap.forEach((k, v) -> {
                try {
                    columnValueMap.put(k, v.get(finalI));
                } catch (Throwable throwable) {
                    throwable.printStackTrace(System.out);
                    System.out.println(columnTypeMap.get(k) + " 列名：" + k + " 读取失败");
                }
            });
            rowList.add(new Row(vin, timeRange.get(i).getTime(), columnValueMap));
        }

        if (rowBuffer != null && rowBuffer.position() > 0 && checkTime < r) {
            // 在行存储的rowBuffer中
            rowBuffer.flip();
            List<Row> notCheckRowList = RowUtil.toRowList(tableSchema, rowBuffer);
            rowBuffer.limit(rowBuffer.capacity());
            for (Row row : notCheckRowList) {
                if (row.getTimestamp() < l || row.getTimestamp() >= r) {
                    continue;
                }
                Map<String, ColumnValue> filteredColumns = new HashMap<>();
                Map<String, ColumnValue> columns = row.getColumns();

                for (String key : requestedColumnSet)
                    filteredColumns.put(key, columns.get(key));
                rowList.add(new Row(vin, row.getTimestamp(), filteredColumns));
            }

        }

        return rowList;
    }

    public void flush() throws IOException {
        timeChannel.flush();
        indexChannel.flush();

        for (ColumnChannel columnChannel : columnChannelMap.values()) {
            columnChannel.flush();
        }
    }

    private void indexOutput() throws IOException {
        ByteBuffer allocate = ByteBuffer.allocate(batchCount * indexItemSize);
        for (int i = 0; i < batchCount; i++) {
            TimeIndexItem timeIndexItem = timeIndexItemList.get(i);
            timeIndexItem.write(allocate);
            for (TableSchema.Column column : tableSchema.getColumnList()) {
                Map<Long, ColumnIndexItem> columnIndexItemMap = columnIndexMap.get(column.columnName);
                ColumnIndexItem columnIndexItem = columnIndexItemMap.get((long) i);
                columnIndexItem.write(allocate);
            }
        }

        byte[] bytes = ByteBufferUtil.zstdEncode(allocate.flip());
        indexChannel.writeBytes(bytes);
        indexChannel.flush();
        indexChannel.close();
    }

    public void shutdown() {
        try {
//            if (!notCheckRowList.isEmpty()) {
//                insert(notCheckRowList);
//            }
            if (rowBuffer != null) {
                if (rowBuffer.position() > 0) {
                    insert();
                }

                CommonUtils.UNSAFE.invokeCleaner(rowBuffer);
                rowChannel.close();
                File rowFile = new File(dir.getAbsolutePath(), "row");
                if (!rowFile.delete()) {
                    System.out.println(("row文件删除失败。"));
                }
            }

            shutdownColumnExecutor();

            ByteBuffer allocate = ByteBuffer.allocate(8);
            allocate.putLong(latestTime);
            allocate.flip();
            metaChannel.write(allocate, 0);
            metaChannel.close();

//            long dirtyColumnIndexItemNum = loadedAllColumnIndexForInit;
//            while (true) {
//                if (tableSchema.getColumnList().isEmpty()) {
//                    break;
//                }
//                for (TableSchema.Column column : tableSchema.getColumnList()) {
//                    Map<Long, ColumnIndexItem> columnIndexItemMap = columnIndexMap.get(column.columnName);
//                    ColumnIndexItem columnIndexItem = columnIndexItemMap.get(dirtyColumnIndexItemNum++);
//                    if (columnIndexItem == null) {
//                        dirtyColumnIndexItemNum = -1;
//                        break;
//                    }
//                    columnIndexChannel.writeBytes(columnIndexItem.toBytes());
//                }
//                if (dirtyColumnIndexItemNum == -1) {
//                    break;
//                }
//            }

            if (batchCount > 0) {
                indexOutput();
            }

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
            return indexChannel.channelSize();
        } catch (IOException e) {
            System.out.println("getColumnIndexFileSize failed.");
            return 0;
        }
    }
}
