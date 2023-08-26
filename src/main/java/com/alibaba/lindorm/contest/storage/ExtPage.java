package com.alibaba.lindorm.contest.storage;

import java.io.IOException;
import java.nio.ByteBuffer;

class ExtPage extends AbPage {

    public ExtPage(VinStorage vinStorage, BufferPool bufferPool, int num) {
        super(vinStorage, bufferPool, num);
    }

    /**
     * 如果当前不是NULL_PAGE，则表示往后还有更多的数据页
     */
    private int nextExtNum;

    @Override
    public synchronized void recover() throws IOException {
        if (stat != PageStat.FLUSHED) {
            return;
        }

        super.recover();

        nextExtNum = dataBuffer.unwrap().getInt();

        stat = PageStat.USING;
    }

    /**
     * 返回连接的下一个扩展页
     *
     * @return
     */
    public int nextExt() {
        return nextExtNum;
    }

    /**
     * 获取扩展页的数据内容
     *
     * @return
     */
    public ByteBuffer getData() {
        return dataBuffer.unwrap().slice();
    }

    /**
     * 写入数据内容到扩展页
     *
     * @param byteBuffer
     */
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
