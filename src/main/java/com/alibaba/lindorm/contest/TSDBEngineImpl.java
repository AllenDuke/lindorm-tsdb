//
// You should modify this file.
//
// Refer TSDBEngineSample.java to ensure that you have understood
// the interface semantics correctly.
//

package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.structs.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TSDBEngineImpl extends TSDBEngine {

    private static class Request {
        volatile Vin vin;
        volatile Row row;
        volatile LatestQueryRequest latestQueryRequest;
        volatile TimeRangeQueryRequest timeRangeQueryRequest;

        volatile ArrayBlockingQueue<Row> latestRow;
        volatile ArrayBlockingQueue<ArrayList<Row>> timeRowList;
    }

    private static final int NUM_FOLDERS = 300;
    private static final ConcurrentMap<Vin, BufferedOutputStream> OUT_FILES = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Vin, Lock> VIN_LOCKS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Vin, Queue<Request>> VIN_Q = new ConcurrentHashMap<>();
    private static final Row NULL_ROW = new Row(null, -1, null);

    private static final BlockingQueue<Request> SCHEDULE_Q = new ArrayBlockingQueue<>(1);

    private static final long CACHE_SIZE = (long) (Runtime.getRuntime().totalMemory() * 0.2);
    private static final AtomicLongFieldUpdater<TSDBEngineImpl> FREE_SIZE_UPDATER = AtomicLongFieldUpdater.newUpdater(TSDBEngineImpl.class, "freeSize");

    private boolean connected = false;
    private int columnsNum;
    private ArrayList<String> columnsName;
    private ArrayList<ColumnValue.ColumnType> columnsType;

    private volatile long freeSize = CACHE_SIZE;

    private final List<Thread> writer = new ArrayList<>();

    private Thread scheduler;

    /**
     * This constructor's function signature should not be modified.
     * Our evaluation program will call this constructor.
     * The function's body can be modified.
     */
    public TSDBEngineImpl(File dataPath) {
        super(dataPath);
        schedule();
        consume();
    }

    @Override
    public void connect() throws IOException {
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
        connected = true;
    }

    @Override
    public void createTable(String tableName, Schema schema) throws IOException {
        Map<String, ColumnValue.ColumnType> columnTypeMap = schema.getColumnTypeMap();

        columnsNum = columnTypeMap.size();
        columnsName = new ArrayList<>();
        columnsType = new ArrayList<>();

        for (Map.Entry<String, ColumnValue.ColumnType> entry : columnTypeMap.entrySet()) {
            columnsName.add(entry.getKey());
            columnsType.add(entry.getValue());
        }
    }

    @Override
    public void shutdown() {
        if (!connected) {
            return;
        }

        // Close all resources, assuming all writing and reading process has finished.
        for (BufferedOutputStream fout : OUT_FILES.values()) {
            try {
                fout.close();
            } catch (IOException e) {
                System.err.println("Error closing outFiles");
                throw new RuntimeException(e);
            }
        }
        OUT_FILES.clear();
        VIN_LOCKS.clear();

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
        connected = false;
    }

    private void consume() {
        int cnt = 4;
        for (int i = 0; i < cnt; i++) {
            Thread thread = new Thread(() -> {
                try {
                    while (true) {
                        for (Map.Entry<Vin, Queue<Request>> entry : VIN_Q.entrySet()) {
                            Vin vin = entry.getKey();
                            if ((vin.hashCode() % cnt) != 0) {
                                // 一个vin智能被一个线程处理
                                continue;
                            }

                            Queue<Request> rowQueue = entry.getValue();
                            if (rowQueue.isEmpty()) {
                                continue;
                            }

                            Lock lock = VIN_LOCKS.computeIfAbsent(vin, key -> new ReentrantLock());
                            lock.lock();
                            Request request = rowQueue.poll();
                            if (request.row != null) {
                                int rowSize = rowSize(request.row);
                                BufferedOutputStream fileOutForVin = getFileOutForVin(vin);
                                appendRowToFile(fileOutForVin, request.row);
                                FREE_SIZE_UPDATER.addAndGet(this, rowSize);
                            } else if (request.latestQueryRequest != null) {
                                FileInputStream fin = getFileInForVin(vin);
                                Row latestRow = null;
                                while (fin.available() > 0) {
                                    Row curRow = readRowFromStream(vin, fin);
                                    if (latestRow == null || curRow.getTimestamp() >= latestRow.getTimestamp()) {
                                        latestRow = curRow;
                                    }
                                }
                                fin.close();
                                if (latestRow != null) {
                                    request.latestRow.put(latestRow);
                                } else {
                                    request.latestRow.put(NULL_ROW);
                                }
                            } else {
                                TimeRangeQueryRequest trReadReq = request.timeRangeQueryRequest;

                                Set<Row> ans = new HashSet<>();

                                FileInputStream fin = getFileInForVin(vin);
                                while (fin.available() > 0) {
                                    Row row = readRowFromStream(vin, fin);
                                    long timestamp = row.getTimestamp();
                                    if (timestamp >= trReadReq.getTimeLowerBound() && timestamp < trReadReq.getTimeUpperBound()) {
                                        Map<String, ColumnValue> filteredColumns = new HashMap<>();
                                        Map<String, ColumnValue> columns = row.getColumns();

                                        for (String key : trReadReq.getRequestedColumns())
                                            filteredColumns.put(key, columns.get(key));
                                        ans.add(new Row(vin, timestamp, filteredColumns));
                                    }
                                }
                                fin.close();

                                request.timeRowList.put(new ArrayList<>(ans));
                            }
                            lock.unlock();
                        }
                    }
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            });
            thread.setName("writer-" + i);
            thread.setDaemon(true);
            thread.start();
            writer.add(thread);
        }
    }

    private void schedule() {
        scheduler = new Thread(() -> {
            while (true) {
                try {
                    Request request = SCHEDULE_Q.take();
                    if (request.row != null) {
                        int rowSize = rowSize(request.row);
                        // 忙等入队
                        while (FREE_SIZE_UPDATER.addAndGet(this, -rowSize) < 0) {
                            FREE_SIZE_UPDATER.addAndGet(this, rowSize);
                        }
                    }

                    Queue<Request> requestQueue = VIN_Q.computeIfAbsent(request.vin, k -> new ConcurrentLinkedQueue<>());
                    requestQueue.add(request);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        scheduler.setName("scheduler");
        scheduler.setDaemon(true);
        scheduler.start();
    }

    @Override
    public void upsert(WriteRequest wReq) throws IOException {
        for (Row row : wReq.getRows()) {
            Request request = new Request();
            request.row = row;
            request.vin = row.getVin();
            try {
                SCHEDULE_Q.put(request);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
        ArrayList<Row> ans = new ArrayList<>();
        for (Vin vin : pReadReq.getVins()) {
            Request request = new Request();
            request.vin = vin;
            request.latestQueryRequest = pReadReq;
            request.latestRow = new ArrayBlockingQueue<>(1);

            Row latestRow = null;
            try {
                SCHEDULE_Q.put(request);
                latestRow = request.latestRow.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (latestRow != null && latestRow != NULL_ROW) {
                Map<String, ColumnValue> filteredColumns = new HashMap<>();
                Map<String, ColumnValue> columns = latestRow.getColumns();
                for (String key : pReadReq.getRequestedColumns())
                    filteredColumns.put(key, columns.get(key));
                ans.add(new Row(vin, latestRow.getTimestamp(), filteredColumns));
            }
        }
        return ans;
    }

    @Override
    public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
        Vin vin = trReadReq.getVin();

        ArrayList<Row> rows = null;

        Request request = new Request();
        request.vin = vin;
        request.timeRangeQueryRequest = trReadReq;
        request.timeRowList = new ArrayBlockingQueue<>(1);

        try {
            SCHEDULE_Q.put(request);
            rows = request.timeRowList.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return rows;
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

    private void appendRowToFile(BufferedOutputStream fout, Row row) {
        if (row.getColumns().size() != columnsNum) {
            System.err.println("Cannot write a non-complete row with columns' num: [" + row.getColumns().size() + "]. ");
            System.err.println("There are [" + columnsNum + "] rows in total");
            throw new RuntimeException();
        }

        try {
            CommonUtils.writeLong(fout, row.getTimestamp());
            for (int i = 0; i < columnsNum; ++i) {
                String cName = columnsName.get(i);
                ColumnValue cVal = row.getColumns().get(cName);
                switch (cVal.getColumnType()) {
                    case COLUMN_TYPE_STRING:
                        CommonUtils.writeString(fout, cVal.getStringValue());
                        break;
                    case COLUMN_TYPE_INTEGER:
                        CommonUtils.writeInt(fout, cVal.getIntegerValue());
                        break;
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        CommonUtils.writeDouble(fout, cVal.getDoubleFloatValue());
                        break;
                    default:
                        throw new IllegalStateException("Invalid column type");
                }
            }
            fout.flush();
        } catch (IOException e) {
            System.err.println("Error writing row to file");
            throw new RuntimeException(e);
        }
    }

    private Row readRowFromStream(Vin vin, FileInputStream fin) {
        try {
            if (fin.available() <= 0) {
                throw new IOException("Premature eof in file for vin: [" + vin
                        + "]. No available data to read");
            }
            long timestamp;
            try {
                timestamp = CommonUtils.readLong(fin);
            } catch (EOFException e) {
                throw new IOException("Premature eof in file for vin: [" + vin
                        + "]. Read timestamp failed");
            }

            Map<String, ColumnValue> columns = new HashMap<>();

            for (int cI = 0; cI < columnsNum; ++cI) {
                String cName = columnsName.get(cI);
                ColumnValue.ColumnType cType = columnsType.get(cI);
                ColumnValue cVal;
                switch (cType) {
                    case COLUMN_TYPE_INTEGER:
                        int intVal = CommonUtils.readInt(fin);
                        cVal = new ColumnValue.IntegerColumn(intVal);
                        break;
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        double doubleVal = CommonUtils.readDouble(fin);
                        cVal = new ColumnValue.DoubleFloatColumn(doubleVal);
                        break;
                    case COLUMN_TYPE_STRING:
                        cVal = new ColumnValue.StringColumn(CommonUtils.readString(fin));
                        break;
                    default:
                        throw new IllegalStateException("Undefined column type, this is not expected");
                }
                columns.put(cName, cVal);
            }
            return new Row(vin, timestamp, columns);
        } catch (IOException e) {
            System.err.println("Error reading row from stream");
            throw new RuntimeException(e);
        }
    }

    private BufferedOutputStream getFileOutForVin(Vin vin) {
        // Try getting from already opened set.
        BufferedOutputStream fileOut = OUT_FILES.get(vin);
        if (fileOut != null) {
            return fileOut;
        }

        // The first time we open the file out stream for this vin, open a new stream and put it into opened set.
        File vinFilePath = getVinFilePath(vin);
        try {
            fileOut = new BufferedOutputStream(new FileOutputStream(vinFilePath, true));
            OUT_FILES.put(vin, fileOut);
            return fileOut;
        } catch (IOException e) {
            System.err.println("Cannot open write stream for vin file: [" + vinFilePath + "]");
            throw new RuntimeException(e);
        }
    }

    private FileInputStream getFileInForVin(Vin vin) {
        // Must be protected by vin's mutex.
        File vinFilePath = getVinFilePath(vin);
        try {
            FileInputStream vinFin = new FileInputStream(vinFilePath);
            return vinFin;
        } catch (FileNotFoundException e) {
            System.err.println("Cannot get vin file input-stream for vin: [" + vin + "]. No such file");
            throw new RuntimeException(e);
        }
    }

    private File getVinFilePath(Vin vin) {
        int folderIndex = vin.hashCode() % NUM_FOLDERS;
        File folder = new File(dataPath, String.valueOf(folderIndex));
        String vinStr = new String(vin.getVin(), StandardCharsets.UTF_8);
        File vinFile = new File(folder.getAbsolutePath(), vinStr);
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
