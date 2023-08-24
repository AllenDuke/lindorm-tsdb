package com.alibaba.lindorm.contest.storage;

import java.nio.channels.FileChannel;

public class PageScheduler {

    private final BufferPool bufferPool;

    public PageScheduler(BufferPool bufferPool) {
        this.bufferPool = bufferPool;
    }

//    public TimeSortedPage apply(FileChannel fileChannel, int num) {
//
//    }
}
