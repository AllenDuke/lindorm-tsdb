//
// You should modify this file.
//
// Refer TSDBEngineSample.java to ensure that you have understood
// the interface semantics correctly.
//

package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.LatestQueryRequest;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Schema;
import com.alibaba.lindorm.contest.structs.TimeRangeQueryRequest;
import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.structs.WriteRequest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TSDBEngineImpl extends TSDBEngine {

  private static final int NUM_FOLDERS = 300;
  private static final ConcurrentMap<Vin, Lock> VIN_LOCKS = new ConcurrentHashMap<>();
  private static final ConcurrentMap<Vin, FileOutputStream> OUT_FILES = new ConcurrentHashMap<>();

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
    for (FileOutputStream fout : OUT_FILES.values()) {
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

  @Override
  public void upsert(WriteRequest wReq) throws IOException {
    for (Row row : wReq.getRows()) {
      Vin vin = row.getVin();
      Lock lock = VIN_LOCKS.computeIfAbsent(vin, key -> new ReentrantLock());
      lock.lock();
      try {
        FileOutputStream fileOutForVin = getFileOutForVin(vin);
        appendRowToFile(fileOutForVin, row);
      } finally {
        lock.unlock();
      }
    }
  }

  @Override
  public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
    ArrayList<Row> ans = new ArrayList<>();
    for (Vin vin : pReadReq.getVins()) {
      Lock lock = VIN_LOCKS.computeIfAbsent(vin, key -> new ReentrantLock());
      lock.lock();

      int rowNums;
      Row latestRow;
      try {
        FileInputStream fin = getFileInForVin(vin);
        latestRow = null;
        rowNums = 0;
        while (fin.available() > 0) {
          Row curRow = readRowFromStream(vin, fin);
          if (rowNums == 0 || curRow.getTimestamp() >= latestRow.getTimestamp()) {
            latestRow = curRow;
          }
          ++rowNums;
        }
        fin.close();
      } finally {
        lock.unlock();
      }

      if (rowNums != 0) {
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
    Set<Row> ans = new HashSet<>();
    Vin vin = trReadReq.getVin();
    Lock lock = VIN_LOCKS.computeIfAbsent(vin, key -> new ReentrantLock());
    lock.lock();

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

  private void appendRowToFile(FileOutputStream fout, Row row) {
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

  private FileOutputStream getFileOutForVin(Vin vin) {
    // Try getting from already opened set.
    FileOutputStream fileOut = OUT_FILES.get(vin);
    if (fileOut != null) {
      return fileOut;
    }

    // The first time we open the file out stream for this vin, open a new stream and put it into opened set.
    File vinFilePath = getVinFilePath(vin);
    try {
      fileOut = new FileOutputStream(vinFilePath, true);
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