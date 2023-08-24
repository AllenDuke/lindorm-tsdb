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
    protected int num;

    /**
     * 当前是否是是数据页
     */
    protected boolean isDataPage;

    /**
     * 当前页所在文件
     */
    protected final FileChannel fileChannel;

    /**
     * 当前页所在的内存池
     */
    protected final BufferPool bufferPool;

    protected ByteBuffer dataBuffer;

    public AbPage(FileChannel fileChannel, BufferPool bufferPool) {
        this.fileChannel = fileChannel;
        this.bufferPool = bufferPool;
    }

    public void recover() throws IOException {
        FileLock lock = fileChannel.lock(PAGE_SIZE * num, PAGE_SIZE, false);
        fileChannel.read(dataBuffer);
    }

    /**
     * 数据刷盘
     */
    public void flush() throws IOException {
        FileLock lock = fileChannel.lock(PAGE_SIZE * num, PAGE_SIZE, false);
        ByteBuffer buffer = bufferPool.allocate(1);
        if (!isDataPage) {
            // cpu分支预测
            buffer.put((byte) 0);
        } else {
            buffer.put((byte) 1);
        }
        buffer.flip();
        fileChannel.write(buffer);
        dataBuffer.flip();
        fileChannel.write(dataBuffer);
        lock.release();
    }
}
