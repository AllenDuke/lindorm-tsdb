package com.alibaba.lindorm.contest.storage;

import java.io.IOException;
import java.nio.channels.FileLock;

public abstract class AbPage {

    /**
     * 页大小4k
     */
    public static long PAGE_SIZE = 16 * 1024;

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

    protected PageStat stat;

    public AbPage(VinStorage vinStorage, BufferPool bufferPool, Integer num) {
        this.vinStorage = vinStorage;
        this.bufferPool = bufferPool;
        this.num = num;
        this.dataBuffer = bufferPool.allocate();
        stat = PageStat.NEW;
    }

    public synchronized void recover() throws IOException {
        if (stat != PageStat.FLUSHED && stat != PageStat.NEW) {
            return;
        }

        dataBuffer = bufferPool.allocate();
        FileLock lock = vinStorage.dbChannel().lock(PAGE_SIZE * num, PAGE_SIZE, false);
        vinStorage.dbChannel().read(dataBuffer.unwrap(), PAGE_SIZE * num);
        lock.release();

        if (dataBuffer.unwrap().position() != dataBuffer.unwrap().capacity()) {
            throw new IllegalStateException("不是一个完整的buffer");
        }

        dataBuffer.unwrap().flip();
        stat = PageStat.RECOVERED;
    }

    /**
     * 数据刷盘
     */
    public synchronized void flush() throws IOException {
        if (stat == PageStat.FLUSHED) {
            return;
        }
        dataBuffer.unwrap().position(0);
        FileLock lock = vinStorage.dbChannel().lock(PAGE_SIZE * num, PAGE_SIZE, false);
        vinStorage.dbChannel().write(dataBuffer.unwrap(), PAGE_SIZE * num);
        lock.release();

        bufferPool.free(dataBuffer);
        stat = PageStat.FLUSHED;
    }
}
