//
// You should modify this file.
//
// Refer TSDBEngineSample.java to ensure that you have understood
// the interface semantics correctly.
//

package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.schedule.PageScheduler;
import com.alibaba.lindorm.contest.storage.VinStorage;
import com.alibaba.lindorm.contest.structs.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PageTSDBEngineImpl extends TSDBEngine {

    private static final Map<Vin, VinStorage> VIN_STORAGE_MAP = new ConcurrentHashMap<>();

    private boolean connected = false;
    private int columnsNum;
    private ArrayList<String> columnsName;
    private ArrayList<ColumnValue.ColumnType> columnsType;

    /**
     * This constructor's function signature should not be modified.
     * Our evaluation program will call this constructor.
     * The function's body can be modified.
     */
    public PageTSDBEngineImpl(File dataPath) {
        super(dataPath);
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
    public synchronized void shutdown() {
        if (!connected) {
            return;
        }

        PageScheduler.PAGE_SCHEDULER.shutdown();

        VIN_STORAGE_MAP.forEach((k, v) -> {
            try {
                v.shutdown();
            } catch (IOException e) {
                e.printStackTrace();
                throw new IllegalStateException("engine shutdown failed.");
            }
        });

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

        connected = false;
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

    @Override
    public void upsert(WriteRequest wReq) throws IOException {
        for (Row row : wReq.getRows()) {
            Row clone = deepClone(row);
            Vin vin = clone.getVin();
            VinStorage vinStorage = VIN_STORAGE_MAP.computeIfAbsent(vin, k -> new VinStorage(vin, dataPath.getPath(), columnsName, columnsType));
            vinStorage.insert(clone);
        }
    }

    @Override
    public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
        ArrayList<Row> ans = new ArrayList<>();
        for (Vin vin : pReadReq.getVins()) {
            VinStorage vinStorage = VIN_STORAGE_MAP.computeIfAbsent(vin, k -> new VinStorage(vin, dataPath.getPath(), columnsName, columnsType));
            Row latestRow = vinStorage.latest();
            if (latestRow == null) {
                continue;
            }
            Map<String, ColumnValue> filteredColumns = new HashMap<>();
            Map<String, ColumnValue> columns = latestRow.getColumns();
            for (String key : pReadReq.getRequestedColumns()) {
                filteredColumns.put(key, columns.get(key));
            }
            ans.add(new Row(vin, latestRow.getTimestamp(), filteredColumns));
        }
        return ans;
    }

    @Override
    public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
        Vin vin = trReadReq.getVin();
        VinStorage vinStorage = VIN_STORAGE_MAP.computeIfAbsent(vin, k -> new VinStorage(vin, dataPath.getPath(), columnsName, columnsType));
        ArrayList<Row> window = vinStorage.window(trReadReq.getTimeLowerBound(), trReadReq.getTimeUpperBound());
        ArrayList<Row> ans = new ArrayList<>(window.size());
        for (Row row : window) {
            Map<String, ColumnValue> filteredColumns = new HashMap<>();
            Map<String, ColumnValue> columns = row.getColumns();
            for (String key : trReadReq.getRequestedColumns()) {
                filteredColumns.put(key, columns.get(key));
            }
            ans.add(new Row(vin, row.getTimestamp(), filteredColumns));
        }
        return ans;
    }
}
