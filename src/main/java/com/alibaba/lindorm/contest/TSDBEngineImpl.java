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

            long timeIndexFileSize = 0;
            for (LsmStorage lsmStorage : LSM_STORAGES.values()) {
                Lock lock = VIN_LOCKS.computeIfAbsent(lsmStorage.getVin(), key -> new ReentrantLock());
                lock.lock();
                timeIndexFileSize += lsmStorage.getTimeIndexFileSize();
                lsmStorage.shutdown();
                lock.unlock();
            }
            LSM_STORAGES.clear();
            VIN_LOCKS.clear();

            System.out.println("shutdown 主键索引总大小：" + timeIndexFileSize + "B");

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

    @Override
    public void write(WriteRequest wReq) throws IOException {
        try {
            for (Row row : wReq.getRows()) {
                Vin vin = row.getVin();
                Lock lock = VIN_LOCKS.computeIfAbsent(vin, key -> new ReentrantLock());
                lock.lock();

                LsmStorage lsmStorage = LSM_STORAGES.computeIfAbsent(vin, v -> new LsmStorage(dataPath, vin, tableSchema));
                lsmStorage.append(row);

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


                Row latestRow;
                try {
                    LsmStorage lsmStorage = LSM_STORAGES.computeIfAbsent(vin, v -> new LsmStorage(dataPath, vin, tableSchema));
                    long latestTimestamp = lsmStorage.getLatestTime();
                    if (latestTimestamp <= 0) {
                        return ans;
                    }
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
}
