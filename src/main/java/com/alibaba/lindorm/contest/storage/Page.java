package com.alibaba.lindorm.contest.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class Page<K extends ByteBuffer, V extends ByteBuffer> extends AbPage {

    public Page(FileChannel fileChannel) {
        super(fileChannel);
        this.isDataPage = false;
    }

    /**
     * 左节点
     */
    private int leftNum;

    /**
     * 当前页最小key
     */
    private K min;

    /**
     * 右节点
     */
    private int rightNum;

    /**
     * 当前页最大key
     */
    private K max;

    private TreeMap<K, V> map = new TreeMap<K, V>();

    @Override
    public void flush() throws IOException {
        dataBuffer.position(0);
        map.forEach((k, v) -> {

        });
        super.flush();
    }

    /**
     * k v会立即开始读
     *
     * @param k
     * @param v
     * @return
     */
    public boolean insert(K k, V v) {
        // 检查当前容量
        if (dataBuffer.remaining() < k.limit() + v.limit()) {
            return false;
        }
        // 插入map
        map.put(k, v);
        k.position(k.limit());
        v.position(v.limit());
        dataBuffer.position(k.limit() + v.limit());
        return true;
    }
}
