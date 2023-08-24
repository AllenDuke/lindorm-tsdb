package com.alibaba.lindorm.contest.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
    protected final FileChannel fileChannel;

    /**
     * 当前页所在的内存池
     */
    protected final BufferPool bufferPool;

    /**
     * flush会释放，
     */
    protected PooledByteBuffer dataBuffer;

    protected boolean recovered;

    public AbPage(FileChannel fileChannel, BufferPool bufferPool, int num) {
        this.fileChannel = fileChannel;
        this.bufferPool = bufferPool;
        this.num = num;
        this.recovered = false;
    }

    public synchronized void recover() throws IOException {
        if (recovered) {
            return;
        }
        dataBuffer = bufferPool.allocate((int) PAGE_SIZE);
        if (fileChannel.size() <= PAGE_SIZE * num) {
            return;
        }
        FileLock lock = fileChannel.lock(PAGE_SIZE * num, PAGE_SIZE, false);
        fileChannel.read(dataBuffer.getByteBuffer(), PAGE_SIZE * num);
        lock.release();

        dataBuffer.getByteBuffer().flip();
        recovered = true;
    }

    /**
     * 数据刷盘
     */
    public void flush() throws IOException {
        FileLock lock = fileChannel.lock(PAGE_SIZE * num, PAGE_SIZE, false);
        fileChannel.write(dataBuffer.getByteBuffer());
        lock.release();

        bufferPool.free(dataBuffer);
    }
}
