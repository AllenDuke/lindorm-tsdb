//
// You should modify this file.
//
// Refer TSDBEngineSample.java to ensure that you have understood
// the interface semantics correctly.
//

package com.alibaba.lindorm.contest;

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
    private static final ConcurrentMap<Vin, OutputStream> DATA_FILES = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Vin, FileChannel> INDEX_FILES = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Vin, Long> DATA_FILE_POSS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Vin, LatestRowInfo> LATEST_ROW_INFOS = new ConcurrentHashMap<>();


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

        // Close all resources, assuming all writing and reading process has finished.
        for (OutputStream fout : DATA_FILES.values()) {
            try {
                fout.flush();
                fout.close();
            } catch (IOException e) {
                System.err.println("Error closing outFiles");
                throw new RuntimeException(e);
            }
        }
        DATA_FILES.clear();
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
        connected = false;
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
    public void upsert(WriteRequest wReq) throws IOException {
        for (Row row : wReq.getRows()) {
            Vin vin = row.getVin();
            Lock lock = VIN_LOCKS.computeIfAbsent(vin, key -> new ReentrantLock());
            lock.lock();
            try {
                Long filePos = DATA_FILE_POSS.computeIfAbsent(vin, k -> 0L);
                OutputStream fileOutForVin = getFileOutForVin(vin);
                appendRowToFile(fileOutForVin, row);

                LatestRowInfo latestRowInfo = LATEST_ROW_INFOS.computeIfAbsent(vin, k -> new LatestRowInfo());
                if (row.getTimestamp() >= latestRowInfo.timestamp) {
                    latestRowInfo.timestamp = row.getTimestamp();
                    latestRowInfo.pos = filePos;
                    latestRowInfo.size = rowSize(row);
                }

                DATA_FILE_POSS.put(vin, filePos + rowSize(row));
            } finally {
                lock.unlock();
            }
        }
    }

    private Row readFormBuffer(Vin vin, ByteBuffer buffer) {
        long timestamp = buffer.getLong();
        Map<String, ColumnValue> columns = new HashMap<>();
        for (int j = 0; j < columnsType.size(); j++) {
            ColumnValue.ColumnType columnType = columnsType.get(j);
            String columnName = columnsName.get(j);
            ColumnValue cVal;
            switch (columnType) {
                case COLUMN_TYPE_INTEGER:
                    int intVal = buffer.getInt();
                    cVal = new ColumnValue.IntegerColumn(intVal);
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    double doubleVal = buffer.getDouble();
                    cVal = new ColumnValue.DoubleFloatColumn(doubleVal);
                    break;
                case COLUMN_TYPE_STRING:
                    int strLen = buffer.getInt();
                    byte[] strBytes = new byte[strLen];
                    buffer.get(strBytes);
                    cVal = new ColumnValue.StringColumn(ByteBuffer.wrap(strBytes));
                    break;
                default:
                    throw new IllegalStateException("Undefined column type, this is not expected");
            }
            columns.put(columnName, cVal);
        }
        return new Row(vin, timestamp, columns);
    }

    @Override
    public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
        ArrayList<Row> ans = new ArrayList<>();
        for (Vin vin : pReadReq.getVins()) {
            Lock lock = VIN_LOCKS.computeIfAbsent(vin, key -> new ReentrantLock());
            lock.lock();

            OutputStream fileOutForVin = getFileOutForVin(vin);
            fileOutForVin.flush();

            FileChannel channel = getIndexFileChannelForVin(vin);
            if (channel.size() == 0) {
                lock.unlock();
                continue;
            }
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, 24);
            long latestTimestamp = buffer.getLong();
            long latestPos = buffer.getLong();
            long latestSize = buffer.getLong();
            // help gc
            buffer = null;

            Row latestRow;
            try {
                FileInputStream fin = getFileInForVin(vin);
                FileChannel fileChannel = fin.getChannel();
                buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, latestPos, latestSize);
                latestRow = readFormBuffer(vin, buffer);
                buffer = null;
                fin.close();
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
    }

    @Override
    public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
        Set<Row> ans = new HashSet<>();
        Vin vin = trReadReq.getVin();
        Lock lock = VIN_LOCKS.computeIfAbsent(vin, key -> new ReentrantLock());
        lock.lock();

        OutputStream fileOutForVin = getFileOutForVin(vin);
        fileOutForVin.flush();

        try {
            FileInputStream fin = getFileInForVin(trReadReq.getVin());
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
        } finally {
            lock.unlock();
        }

        return new ArrayList<>(ans);
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

    private void appendRowToFile(OutputStream fout, Row row) {
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
//            fout.flush();
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

    private OutputStream getFileOutForVin(Vin vin) {
        // Try getting from already opened set.
        OutputStream fileOut = DATA_FILES.get(vin);
        if (fileOut != null) {
            return fileOut;
        }

        // The first time we open the file out stream for this vin, open a new stream and put it into opened set.
        File vinFilePath = getVinFilePath(vin);
        try {
            fileOut = new BufferedOutputStream(new FileOutputStream(vinFilePath, true));
            DATA_FILES.put(vin, fileOut);
            return fileOut;
        } catch (IOException e) {
            System.err.println("Cannot open write stream for vin file: [" + vinFilePath + "]");
            throw new RuntimeException(e);
        }
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
