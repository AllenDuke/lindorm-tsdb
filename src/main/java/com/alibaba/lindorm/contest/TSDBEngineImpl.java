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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TSDBEngineImpl extends TSDBEngine {

    static {
        System.out.println(Runtime.getRuntime().totalMemory() / 1024 / 1024 / 1024 + "GB");
    }

    private static final ConcurrentMap<Vin, ReentrantReadWriteLock> VIN_LOCKS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Vin, LsmStorage> LSM_STORAGES = new ConcurrentHashMap<>();
    private volatile boolean connected = false;
    private int columnsNum;
    private ArrayList<String> columnsName;
    private ArrayList<ColumnValue.ColumnType> columnsType;
    private TableSchema tableSchema;

    private Thread flusher;

    private void initFlusher() {
        flusher = new Thread(() -> {
            while (connected) {
                try {
                    Thread.sleep(3000);
                    for (Vin vin : VIN_LOCKS.keySet()) {
                        LsmStorage lsmStorage = LSM_STORAGES.get(vin);
                        if (lsmStorage == null) {
                            continue;
                        }
                        ReentrantReadWriteLock lock = VIN_LOCKS.get(vin);
                        if (lock == null) {
                            continue;
                        }
                        lock.writeLock().lock();
                        try {
                            lsmStorage.flush();
                        } finally {
                            lock.writeLock().unlock();
                        }
                    }
                } catch (Throwable throwable) {
                    throwable.printStackTrace(System.out);
                }
            }
            System.out.println("flusher out");
        }, "flusher");
        flusher.setDaemon(true);
//        flusher.start();
    }


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
                initFlusher();
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
            initFlusher();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            System.out.println("connect failed.");
            throwable.printStackTrace(System.out);
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

            StringBuilder stringBuilder = new StringBuilder("columnName:");
            for (String s : columnsName) {
                stringBuilder.append(s).append(" ");
            }
            System.out.println(stringBuilder);

            stringBuilder.setLength(0);
            stringBuilder.append("columnType:");
            for (ColumnValue.ColumnType columnType : columnsType) {
                stringBuilder.append(columnType.name()).append(" ");
            }
            System.out.println(stringBuilder);

            tableSchema = new TableSchema(columnsName, columnsType);
        } catch (Throwable throwable) {
            System.out.println("createTable failed.");
            throwable.printStackTrace(System.out);
            throw throwable;
        }
    }

    @Override
    public void shutdown() {
        try {
            if (!connected) {
                return;
            }

            AtomicLong timeIndexFileSize = new AtomicLong(0);
            CountDownLatch countDownLatch = new CountDownLatch(LSM_STORAGES.size());
            ThreadPoolExecutor shutdownExecutor = new ThreadPoolExecutor(100, 100, 3L, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(10000), new ThreadPoolExecutor.CallerRunsPolicy());
            for (LsmStorage lsmStorage : LSM_STORAGES.values()) {
                shutdownExecutor.execute(() -> {
                    ReentrantReadWriteLock lock = VIN_LOCKS.computeIfAbsent(lsmStorage.getVin(), key -> new ReentrantReadWriteLock());
                    lock.writeLock().lock();
                    timeIndexFileSize.getAndAdd(lsmStorage.getTimeIndexFileSize());
                    lsmStorage.shutdown();
                    lock.writeLock().unlock();
                    countDownLatch.countDown();
                });
            }
            shutdownExecutor.shutdown();
            countDownLatch.await();
            LSM_STORAGES.clear();
            VIN_LOCKS.clear();

            System.out.println("shutdown 主键索引总大小：" + timeIndexFileSize.get() + "B");

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
            System.out.println("shutdown failed.");
            throwable.printStackTrace(System.out);
            throw new RuntimeException(throwable);
        }
    }

    @Override
    public void write(WriteRequest wReq) throws IOException {
        try {
            for (Row row : wReq.getRows()) {
                Vin vin = row.getVin();
                ReentrantReadWriteLock lock = VIN_LOCKS.computeIfAbsent(vin, key -> new ReentrantReadWriteLock());
                lock.writeLock().lock();

                LsmStorage lsmStorage = LSM_STORAGES.computeIfAbsent(vin, v -> new LsmStorage(dataPath, vin, tableSchema));
                lsmStorage.append(row);

                lock.writeLock().unlock();
            }
        } catch (Throwable throwable) {
            System.out.println("write failed.");
            throwable.printStackTrace(System.out);
            throw throwable;
        }
    }

    @Override
    public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
        try {
            ArrayList<Row> ans = new ArrayList<>();
            for (Vin vin : pReadReq.getVins()) {
                ReentrantReadWriteLock lock = VIN_LOCKS.computeIfAbsent(vin, key -> new ReentrantReadWriteLock());
                lock.writeLock().lock();

                Row latestRow;
                try {
                    LsmStorage lsmStorage = LSM_STORAGES.computeIfAbsent(vin, v -> new LsmStorage(dataPath, vin, tableSchema));
                    latestRow = lsmStorage.getLatestRow();
                    if (latestRow == null) {
                        return ans;
                    }
                } finally {
                    lock.writeLock().unlock();
                }

                Map<String, ColumnValue> filteredColumns = new HashMap<>();
                Map<String, ColumnValue> columns = latestRow.getColumns();
                for (String key : pReadReq.getRequestedColumns())
                    filteredColumns.put(key, columns.get(key));
                ans.add(new Row(vin, latestRow.getTimestamp(), filteredColumns));

            }
            return ans;
        } catch (Throwable throwable) {
            System.out.println("executeLatestQuery failed.");
            throwable.printStackTrace(System.out);
            throw throwable;
        }
    }

    @Override
    public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
        try {
            Vin vin = trReadReq.getVin();
            ReentrantReadWriteLock lock = VIN_LOCKS.computeIfAbsent(vin, key -> new ReentrantReadWriteLock());
            lock.writeLock().lock();

            LsmStorage lsmStorage = LSM_STORAGES.computeIfAbsent(vin, v -> new LsmStorage(dataPath, vin, tableSchema));
            ArrayList<Row> range = lsmStorage.range(trReadReq.getTimeLowerBound(), trReadReq.getTimeUpperBound(), trReadReq.getRequestedColumns());
            lock.writeLock().unlock();
            return range;
        } catch (Throwable throwable) {
            System.out.println("executeTimeRangeQuery failed, l:" + trReadReq.getTimeLowerBound() + ", r:" + trReadReq.getTimeUpperBound());
            throwable.printStackTrace(System.out);
            throw throwable;
        }
    }

    @Override
    public ArrayList<Row> executeAggregateQuery(TimeRangeAggregationRequest aggregationReq) throws IOException {
        try {
            Vin vin = aggregationReq.getVin();
            ReentrantReadWriteLock lock = VIN_LOCKS.computeIfAbsent(vin, key -> new ReentrantReadWriteLock());
            lock.writeLock().lock();

            ArrayList<Row> rows = new ArrayList<>();

            LsmStorage lsmStorage = LSM_STORAGES.computeIfAbsent(vin, v -> new LsmStorage(dataPath, vin, tableSchema));
            Row row = lsmStorage.agg(aggregationReq.getTimeLowerBound(), aggregationReq.getTimeUpperBound(), aggregationReq.getColumnName(), aggregationReq.getAggregator(), null);
            if (row != null) {
                rows.add(row);
            }
            lock.writeLock().unlock();
            return rows;
        } catch (Throwable throwable) {
            System.out.println("executeTimeRangeQuery failed, l:" + aggregationReq.getTimeLowerBound()
                    + ", r:" + aggregationReq.getTimeUpperBound() + ", agg:" + aggregationReq.getAggregator().name());
            throwable.printStackTrace(System.out);
            throw throwable;
        }
    }

    @Override
    public ArrayList<Row> executeDownsampleQuery(TimeRangeDownsampleRequest downsampleReq) throws IOException {
        try {
            Vin vin = downsampleReq.getVin();
            ReentrantReadWriteLock lock = VIN_LOCKS.computeIfAbsent(vin, key -> new ReentrantReadWriteLock());
            lock.writeLock().lock();

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
            lock.writeLock().unlock();
            return rows;
        } catch (Throwable throwable) {
            System.out.println("executeDownsampleQuery failed, l:" + downsampleReq.getTimeLowerBound()
                    + ", r:" + downsampleReq.getTimeUpperBound() + ", agg:" + downsampleReq.getAggregator().name()
                    + ", interval:" + downsampleReq.getInterval());
            throwable.printStackTrace(System.out);
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
