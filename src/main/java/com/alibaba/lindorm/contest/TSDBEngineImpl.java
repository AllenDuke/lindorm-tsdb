//
// You should modify this file.
//
// Refer TSDBEngineSample.java to ensure that you have understood
// the interface semantics correctly.
//

package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.lsm.LsmStorage;
import com.alibaba.lindorm.contest.lsm.TableSchema;
import com.alibaba.lindorm.contest.structs.*;

import java.io.*;
import java.lang.ref.Cleaner;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TSDBEngineImpl extends TSDBEngine {

    private static class LatestRowInfo {
        volatile long timestamp;
        volatile long pos;
        volatile long size;
    }

    private static final int NUM_FOLDERS = 300;
    private static final ConcurrentMap<Vin, Lock> VIN_LOCKS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Vin, LsmStorage> LSM_STORAGES = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Vin, FileChannel> INDEX_FILES = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Vin, Long> DATA_FILE_POSS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Vin, LatestRowInfo> LATEST_ROW_INFOS = new ConcurrentHashMap<>();


    private boolean connected = false;
    private int columnsNum;
    private ArrayList<String> columnsName;
    private ArrayList<ColumnValue.ColumnType> columnsType;
    private TableSchema tableSchema;

    /**
     * This constructor's function signature should not be modified.
     * Our evaluation program will call this constructor.
     * The function's body can be modified.
     */
    public TSDBEngineImpl(File dataPath) {
        super(dataPath);
    }

    @Override
    public void connect() throws IOException {
        try {
            if (connected) {
                throw new IOException("Connected");
            }
            File schemaFile = new File(dataPath, "schema.txt");
            if (!schemaFile.exists() || !schemaFile.isFile()) {
                System.out.println("Connect new database with empty pre-written data");
                connected = true;
                return;
            }
            try (BufferedReader reader = new BufferedReader(new FileReader(schemaFile))) {
                String line;
                if ((line = reader.readLine()) != null && !line.isEmpty()) {
                    String[] parts = line.split(",");
                    columnsNum = Integer.parseInt(parts[0]);
                    if (columnsNum <= 0) {
                        System.err.println("Unexpected columns' num: [" + columnsNum + "]");
                        throw new RuntimeException();
                    }
                    columnsName = new ArrayList<>();
                    columnsType = new ArrayList<>();
                    int index = 1;
                    for (int i = 0; i < columnsNum; i++) {
                        columnsName.add(parts[index++]);
                        columnsType.add(ColumnValue.ColumnType.valueOf(parts[index++]));
                    }
                }
            }
            tableSchema = new TableSchema(columnsName, columnsType);
            connected = true;
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            throw throwable;
        }
    }

    @Override
    public void createTable(String tableName, Schema schema) throws IOException {
        try {
            Map<String, ColumnValue.ColumnType> columnTypeMap = schema.getColumnTypeMap();

            columnsNum = columnTypeMap.size();
            columnsName = new ArrayList<>();
            columnsType = new ArrayList<>();

            for (Map.Entry<String, ColumnValue.ColumnType> entry : columnTypeMap.entrySet()) {
                columnsName.add(entry.getKey());
                columnsType.add(entry.getValue());
            }

            tableSchema = new TableSchema(columnsName, columnsType);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            throw throwable;
        }
    }

    @Override
    public void shutdown() {
        try {
            if (!connected) {
                return;
            }

            for (LsmStorage lsmStorage : LSM_STORAGES.values()) {
                Lock lock = VIN_LOCKS.computeIfAbsent(lsmStorage.getVin(), key -> new ReentrantLock());
                lock.lock();
                lsmStorage.shutdown();
                lock.unlock();
            }
            LSM_STORAGES.clear();
            VIN_LOCKS.clear();

            for (Map.Entry<Vin, LatestRowInfo> entry : LATEST_ROW_INFOS.entrySet()) {
                Vin vin = entry.getKey();
                LatestRowInfo latestRowInfo = entry.getValue();
                FileChannel fileChannel = getIndexFileChannelForVin(vin);
                try {
                    MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 24);
                    buffer.putLong(latestRowInfo.timestamp);
                    buffer.putLong(latestRowInfo.pos);
                    buffer.putLong(latestRowInfo.size);
                    buffer.flip();
                    buffer.force();
                    fileChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            INDEX_FILES.clear();

            // Persist the schema.
            try {
                File schemaFile = new File(getDataPath(), "schema.txt");
                schemaFile.delete();
                schemaFile.createNewFile();
                BufferedWriter writer = new BufferedWriter(new FileWriter(schemaFile));
                writer.write(schemaToString());
                writer.close();
            } catch (IOException e) {
                System.err.println("Error saving the schema");
                throw new RuntimeException(e);
            }
            columnsName.clear();
            columnsType.clear();
            tableSchema = null;
            connected = false;
            System.out.println("shutdown done");
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            throw throwable;
        }
    }

    private int rowSize(Row row) {
        // todo 暂不计算vin
        int size = 0;

        // 时间戳
        size += 8;

        for (String columnName : columnsName) {
            ColumnValue columnValue = row.getColumns().get(columnName);
            switch (columnValue.getColumnType()) {
                case COLUMN_TYPE_STRING:
                    size += 4;
                    size += columnValue.getStringValue().remaining();
                    break;
                case COLUMN_TYPE_INTEGER:
                    size += 4;
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    size += 8;
                    break;
                default:
                    throw new IllegalStateException("Invalid column type");
            }
        }

        return size;
    }

    @Override
    public void write(WriteRequest wReq) throws IOException {
        try {
            for (Row row : wReq.getRows()) {
                Vin vin = row.getVin();
                Lock lock = VIN_LOCKS.computeIfAbsent(vin, key -> new ReentrantLock());
                lock.lock();

                Long filePos = DATA_FILE_POSS.computeIfAbsent(vin, k -> 0L);
                LsmStorage lsmStorage = LSM_STORAGES.computeIfAbsent(vin, v -> new LsmStorage(dataPath, vin, tableSchema));
                lsmStorage.append(row);

                LatestRowInfo latestRowInfo = LATEST_ROW_INFOS.computeIfAbsent(vin, k -> new LatestRowInfo());
                if (row.getTimestamp() >= latestRowInfo.timestamp) {
                    latestRowInfo.timestamp = row.getTimestamp();
                    latestRowInfo.pos = filePos;
                    latestRowInfo.size = rowSize(row);
                }

                DATA_FILE_POSS.put(vin, filePos + rowSize(row));

                lock.unlock();
            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            throw throwable;
        }
    }

    @Override
    public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
        try {
            ArrayList<Row> ans = new ArrayList<>();
            for (Vin vin : pReadReq.getVins()) {
                Lock lock = VIN_LOCKS.computeIfAbsent(vin, key -> new ReentrantLock());
                lock.lock();

                long latestTimestamp;
                long latestPos;
                long latestSize;
                LatestRowInfo latestRowInfo = LATEST_ROW_INFOS.get(vin);
                if (latestRowInfo != null) {
                    // 还在内存中
                    latestTimestamp = latestRowInfo.timestamp;
                    latestPos = latestRowInfo.pos;
                    latestSize = latestRowInfo.size;
                } else {
                    FileChannel indexChannel = getIndexFileChannelForVin(vin);
                    if (indexChannel.size() == 0) {
                        lock.unlock();
                        continue;
                    }
                    ByteBuffer buffer = ByteBuffer.allocate(24);
                    indexChannel.read(buffer, 0);
                    buffer.flip();
                    latestTimestamp = buffer.getLong();
                    latestPos = buffer.getLong();
                    latestSize = buffer.getLong();
                    // help gc
                    buffer = null;

                    latestRowInfo = new LatestRowInfo();
                    latestRowInfo.timestamp = latestTimestamp;
                    latestRowInfo.pos = latestPos;
                    latestRowInfo.size = latestSize;
                    LATEST_ROW_INFOS.put(vin, latestRowInfo);
                }

                Row latestRow;
                try {
                    LsmStorage lsmStorage = LSM_STORAGES.computeIfAbsent(vin, v -> new LsmStorage(dataPath, vin, tableSchema));
                    latestRow = lsmStorage.range(latestTimestamp, latestTimestamp + 1).get(0);
                } finally {
                    lock.unlock();
                }

                Map<String, ColumnValue> filteredColumns = new HashMap<>();
                Map<String, ColumnValue> columns = latestRow.getColumns();
                for (String key : pReadReq.getRequestedColumns())
                    filteredColumns.put(key, columns.get(key));
                ans.add(new Row(vin, latestRow.getTimestamp(), filteredColumns));

            }
            return ans;
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            throw throwable;
        }
    }

    @Override
    public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
        try {
            Set<Row> ans = new HashSet<>();
            Vin vin = trReadReq.getVin();
            Lock lock = VIN_LOCKS.computeIfAbsent(vin, key -> new ReentrantLock());
            lock.lock();

            LsmStorage lsmStorage = LSM_STORAGES.computeIfAbsent(vin, v -> new LsmStorage(dataPath, vin, tableSchema));
            List<Row> range = lsmStorage.range(trReadReq.getTimeLowerBound(), trReadReq.getTimeUpperBound());
            for (Row row : range) {
                Map<String, ColumnValue> filteredColumns = new HashMap<>();
                Map<String, ColumnValue> columns = row.getColumns();

                for (String key : trReadReq.getRequestedColumns())
                    filteredColumns.put(key, columns.get(key));
                ans.add(new Row(vin, row.getTimestamp(), filteredColumns));
            }

            lock.unlock();
            return new ArrayList<>(ans);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            throw throwable;
        }
    }

    @Override
    public ArrayList<Row> executeAggregateQuery(TimeRangeAggregationRequest aggregationReq) throws IOException {
        try {
            Vin vin = aggregationReq.getVin();
            Lock lock = VIN_LOCKS.computeIfAbsent(vin, key -> new ReentrantLock());
            lock.lock();

            ArrayList<Row> rows = new ArrayList<>();

            LsmStorage lsmStorage = LSM_STORAGES.computeIfAbsent(vin, v -> new LsmStorage(dataPath, vin, tableSchema));
            Row row = lsmStorage.agg(aggregationReq.getTimeLowerBound(), aggregationReq.getTimeUpperBound(), aggregationReq.getColumnName(), aggregationReq.getAggregator(), null);
            if (row != null) {
                rows.add(row);
            }
            lock.unlock();
            return rows;
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            throw throwable;
        }
    }

    @Override
    public ArrayList<Row> executeDownsampleQuery(TimeRangeDownsampleRequest downsampleReq) throws IOException {
        try {
            Vin vin = downsampleReq.getVin();
            Lock lock = VIN_LOCKS.computeIfAbsent(vin, key -> new ReentrantLock());
            lock.lock();

            ArrayList<Row> rows = new ArrayList<>();

            LsmStorage lsmStorage = LSM_STORAGES.computeIfAbsent(vin, v -> new LsmStorage(dataPath, vin, tableSchema));
            long l = downsampleReq.getTimeLowerBound();
            long r = Math.min(l + downsampleReq.getInterval(), downsampleReq.getTimeUpperBound());
            while (l < downsampleReq.getTimeUpperBound()) {
                Row row = lsmStorage.agg(l, r, downsampleReq.getColumnName(), downsampleReq.getAggregator(), downsampleReq.getColumnFilter());
                if (row != null) {
                    rows.add(row);
                }
                l = r;
                r = Math.min(l + downsampleReq.getInterval(), downsampleReq.getTimeUpperBound());
            }

            lock.unlock();
            return rows;
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            throw throwable;
        }
    }

    private String schemaToString() {
        StringBuilder sb = new StringBuilder();
        sb.append(columnsNum);
        for (int i = 0; i < columnsNum; ++i) {
            sb.append(",")
                    .append(columnsName.get(i))
                    .append(",")
                    .append(columnsType.get(i));
        }
        return sb.toString();
    }

    private FileChannel getIndexFileChannelForVin(Vin vin) {
        // Try getting from already opened set.
        FileChannel fileOut = INDEX_FILES.get(vin);
        if (fileOut != null) {
            return fileOut;
        }

        // The first time we open the file out stream for this vin, open a new stream and put it into opened set.
        File vinFilePath = getVinIndexFilePath(vin);
        try {
            fileOut = new RandomAccessFile(vinFilePath, "rw").getChannel();
            INDEX_FILES.put(vin, fileOut);
            return fileOut;
        } catch (IOException e) {
            System.err.println("Cannot open write stream for vin file: [" + vinFilePath + "]");
            throw new RuntimeException(e);
        }
    }

    private File getVinIndexFilePath(Vin vin) {
        int folderIndex = vin.hashCode() % NUM_FOLDERS;
        File folder = new File(dataPath, String.valueOf(folderIndex));
        String vinStr = new String(vin.getVin(), StandardCharsets.UTF_8);
        File vinFile = new File(folder.getAbsolutePath(), vinStr + ".idx");
        if (!folder.exists()) {
            folder.mkdirs();
        }
        if (!vinFile.exists()) {
            try {
                vinFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return vinFile;
    }
}
