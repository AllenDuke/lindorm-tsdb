package com.alibaba.lindorm.contest.storage;

import java.io.IOException;
import java.nio.channels.FileLock;

public abstract class AbPage {

    /**
     * 页大小4k
     */
    public static long PAGE_SIZE = 4 * 1024;

    /**
     * 表示往后没有更多的页
     */
    public static int NULL_PAGE = -1;

    /**
     * 页号
     */
    protected final int num;

    /**
     * 当前页所在文件
     */
    protected final VinStorage vinStorage;

    /**
     * 当前页所在的内存池
     */
    protected final BufferPool bufferPool;

    /**
     * flush会释放，
     */
    protected PooledByteBuffer dataBuffer;

    protected boolean recovered;

    public AbPage(VinStorage vinStorage, BufferPool bufferPool, int num) {
        this.vinStorage = vinStorage;
        this.bufferPool = bufferPool;
        this.num = num;
        this.recovered = false;
    }

    public synchronized void recover() throws IOException {
        if (recovered) {
            return;
        }
        dataBuffer = bufferPool.allocate((int) PAGE_SIZE);
        if (vinStorage.size() <= PAGE_SIZE * num) {
            recovered = true;
            return;
        }
        FileLock lock = vinStorage.dbChannel().lock(PAGE_SIZE * num, PAGE_SIZE, false);
        vinStorage.dbChannel().read(dataBuffer.unwrap(), PAGE_SIZE * num);
        lock.release();

        if (dataBuffer.unwrap().position() != dataBuffer.unwrap().capacity()) {
            throw new IllegalStateException("不是一个完整的buffer");
        }

        dataBuffer.unwrap().flip();
        recovered = true;
    }

    /**
     * 数据刷盘
     */
    public void flush() throws IOException {
        FileLock lock = vinStorage.dbChannel().lock(PAGE_SIZE * num, PAGE_SIZE, false);
        vinStorage.dbChannel().write(dataBuffer.unwrap());
        lock.release();

        bufferPool.free(dataBuffer);
    }
}
