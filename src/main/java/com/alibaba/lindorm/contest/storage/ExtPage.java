package com.alibaba.lindorm.contest.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

class ExtPage extends AbPage {

    public ExtPage(FileChannel fileChannel, BufferPool bufferPool, int num) {
        super(fileChannel, bufferPool, num);
    }

    /**
     * 如果当前不是NULL_PAGE，则表示往后还有更多的数据页
     */
    private int nextNum;

    @Override
    public synchronized void recover() throws IOException {
        super.recover();

        nextNum = dataBuffer.getByteBuffer().getInt();
    }

    public int next() {
        return nextNum;
    }
}
