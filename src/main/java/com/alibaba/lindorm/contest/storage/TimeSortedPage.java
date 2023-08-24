package com.alibaba.lindorm.contest.storage;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TimeSortedPage extends AbPage {

    public static final Map<FileChannel, AtomicInteger> FILE_MAX_PAGE_NUM = new ConcurrentHashMap<>();

    public TimeSortedPage(FileChannel fileChannel, BufferPool bufferPool, int num) {
        super(fileChannel, bufferPool, num);
    }

    /**
     * 左节点
     */
    private int leftNum;

    /**
     * 当前页最小key
     */
    private long minTime;

    /**
     * 右节点
     */
    private int rightNum;

    /**
     * 当前页最大key
     */
    private long maxTime;

    private TreeMap<Long, Row> map = new TreeMap<>();

    private List<ExtPage> extPageList;

    @Override
    public synchronized void recover() throws IOException {
        if (recovered) {
            return;
        }

        super.recover();

        leftNum = dataBuffer.getByteBuffer().getInt();
        minTime = dataBuffer.getByteBuffer().getLong();
        rightNum = dataBuffer.getByteBuffer().getInt();
        maxTime = dataBuffer.getByteBuffer().getLong();

        // 实际上也不会有循环recover
        int nextNum = dataBuffer.getByteBuffer().getInt();
        while (nextNum >= 0 && nextNum != num) {
            // 有扩展页
            ExtPage extPage = new ExtPage(fileChannel, bufferPool, nextNum);
            extPage.recover();
            extPageList.add(extPage);

            nextNum = extPage.next();
        }
        recovered = true;
    }

    @Override
    public void flush() throws IOException {
        for (Row row : map.values()) {
            dataBuffer.getByteBuffer().putInt(row.totalSize());
            Map<String, ColumnValue> columns = row.getColumns();
            for (ColumnValue cVal : columns.values()) {
                switch (cVal.getColumnType()) {
                    case COLUMN_TYPE_STRING:
                        dataBuffer.getByteBuffer().put(cVal.getStringValue());
                        break;
                    case COLUMN_TYPE_INTEGER:
                        dataBuffer.getByteBuffer().putInt(cVal.getIntegerValue());
                        break;
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        dataBuffer.getByteBuffer().putDouble(cVal.getDoubleFloatValue());
                        break;
                    default:
                        throw new IllegalStateException("Invalid column type");
                }
            }
        }
        super.flush();

        // 释放内存
        map.clear();
        extPageList.clear();
    }

    /**
     * 插入大数据
     *
     * @param k
     * @param v
     */
    private void insertLarge(long k, Row v, int vTotalSize) {
        extPageList = new LinkedList<>();

    }

    /**
     * k v会立即开始读
     *
     * @param k
     * @param v
     * @return
     */
    public boolean insert(long k, Row v) throws IOException {
        recover();
        // 检查是否插入
        if (map.isEmpty()) {
            // todo first
            return true;
        }

        if (v.getTimestamp() < minTime || v.getTimestamp() > maxTime) {
            return false;
        }

        // 检查当前容量
        int position = dataBuffer.getByteBuffer().position();
        // 4字节记录行数据大小，接着记录行数据
        int vTotalSize = v.totalSize();
        if (dataBuffer.getByteBuffer().remaining() < 4 + vTotalSize) {
            if (position == 0) {
                insertLarge(k, v, vTotalSize);
                return true;
            }
            return false;
        }
        // 插入map
        map.put(k, v);

        dataBuffer.getByteBuffer().position(position + 4 + vTotalSize);
        return true;
    }
}
