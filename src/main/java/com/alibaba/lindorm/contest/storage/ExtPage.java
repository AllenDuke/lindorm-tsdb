package com.alibaba.lindorm.contest.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

class ExtPage extends AbPage {

    public ExtPage(VinStorage vinStorage, BufferPool bufferPool, int num) {
        super(vinStorage, bufferPool, num);
    }

    /**
     * 如果当前不是NULL_PAGE，则表示往后还有更多的数据页
     */
    private int nextNum;

    @Override
    public synchronized void recover() throws IOException {
        if (stat != PageStat.FLUSHED) {
            return;
        }

        super.recover();

        nextNum = dataBuffer.unwrap().getInt();

        stat = PageStat.USING;
    }

    public int next() {
        return nextNum;
    }

    public ByteBuffer getData() {
        return dataBuffer.unwrap().slice();
    }

    public void putData(ByteBuffer byteBuffer) {
        dataBuffer.unwrap().put(byteBuffer);
    }

    /**
     * 可存储的数据内容。
     *
     * @return
     */
    public int dataCapacity() {
        return dataBuffer.unwrap().remaining();
    }
}
