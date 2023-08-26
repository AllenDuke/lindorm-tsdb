//
// You should modify this file.
//
// Refer TSDBEngineSample.java to ensure that you have understood
// the interface semantics correctly.
//

package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.storage.VinStorage;
import com.alibaba.lindorm.contest.structs.*;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TSDBEngineImpl extends TSDBEngine {

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
    public TSDBEngineImpl(File dataPath) {
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
    public void shutdown() {
        if (!connected) {
            return;
        }

        VIN_STORAGE_MAP.forEach((k, v) -> {
            try {
                v.shutdown();
            } catch (IOException e) {
                e.printStackTrace();
                throw new IllegalStateException("engine shutdown failed.");
            }
        });

        columnsName.clear();
        columnsType.clear();
        connected = false;
    }

    @Override
    public void upsert(WriteRequest wReq) throws IOException {
        for (Row row : wReq.getRows()) {
            Vin vin = row.getVin();
            VinStorage vinStorage = VIN_STORAGE_MAP.computeIfAbsent(vin, k -> new VinStorage(vin, dataPath.getPath(), columnsName, columnsType));
            vinStorage.insert(row);
        }
    }

    @Override
    public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
        ArrayList<Row> ans = new ArrayList<>();
        for (Vin vin : pReadReq.getVins()) {
            VinStorage vinStorage = VIN_STORAGE_MAP.get(vin);
            if (vinStorage == null) {
                continue;
            }
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
        VinStorage vinStorage = VIN_STORAGE_MAP.get(vin);
        if (vinStorage == null) {
            return new ArrayList<>(0);
        }
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
