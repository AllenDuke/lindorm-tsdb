package com.alibaba.lindorm.contest.storage;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.schedule.PageScheduler;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.*;
import java.lang.reflect.Constructor;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

public class WriteFirstVinStorage {

    private final Vin vin;

    private File dbFile;

    private FileChannel dbChannel;

    private File indexFile;

    private final String path;

    private final ArrayList<String> columnNameList;

    private final ArrayList<ColumnValue.ColumnType> columnTypeList;

    /**
     * 保存最新的
     */
    private long latestRowKey = -1;

    /**
     * 单个vin文件串行读写
     */
    private final Lock vinLock = new ReentrantLock();

    public WriteFirstVinStorage(Vin vin, String path, ArrayList<String> columnNameList, ArrayList<ColumnValue.ColumnType> columnTypeList) throws IOException {
        this.vin = vin;
        this.path = path;
        this.columnNameList = columnNameList;
        this.columnTypeList = columnTypeList;
        init();
    }

    private void init() throws IOException {
        String vinStr = new String(vin.getVin(), StandardCharsets.UTF_8);
        dbFile = new File(path, vinStr + ".db");
        indexFile = new File(path, vinStr + ".idx");
        if (!dbFile.exists()) {
            dbFile.createNewFile();
        }
        if (!indexFile.exists()) {
            indexFile.createNewFile();
        }
        dbChannel = new RandomAccessFile(dbFile, "rw").getChannel();
        if (dbChannel.size() > 0) {
            // 从文件中恢复
            FileInputStream inputStream = new FileInputStream(indexFile);
            latestRowKey = CommonUtils.readLong(inputStream);
            inputStream.close();
        }
    }

    public ArrayList<Row> window(long minTime, long maxTime) throws IOException {
        vinLock.lock();
        init();


        WindowSearchRequest request = new WindowSearchRequest(minTime, maxTime);
        ArrayList<Row> rows = new ArrayList<>();

        TimeSortedPage cur;

        vinLock.unlock();
        return rows;
    }

    public Row latest() throws IOException {
        vinLock.lock();
        init();
        ArrayList<Row> window = window(latestRowKey, latestRowKey + 1);
        if (window.isEmpty()) {
            vinLock.unlock();
            return null;
        }
        vinLock.unlock();
        return window.get(0);
    }

    public boolean insert(Row row) throws IOException {
        vinLock.lock();

        Vin vin = row.getVin();
        if (!this.vin.equals(vin)) {
            vinLock.unlock();
            return false;
        }

        init();

        if (latestRowKey == -1 || row.getTimestamp() >= latestRowKey) {
            latestRowKey = row.getTimestamp();
        }

        Queue<Row> rowQueue = new LinkedList<>();
        rowQueue.add(row);


        vinLock.unlock();
        return true;
    }

    public void shutdown() throws IOException {
        vinLock.lock();

        FileOutputStream outputStream = new FileOutputStream(indexFile);
        CommonUtils.writeLong(outputStream, latestRowKey);
        outputStream.flush();
        outputStream.close();
        dbChannel.close();
        vinLock.unlock();
    }

}
