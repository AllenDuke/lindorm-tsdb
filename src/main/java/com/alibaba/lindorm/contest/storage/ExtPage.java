package com.alibaba.lindorm.contest.storage;

import java.io.IOException;
import java.nio.ByteBuffer;

class ExtPage extends AbPage {

    public ExtPage(VinStorage vinStorage, BufferPool bufferPool, Integer num) {
        super(vinStorage, bufferPool, num);
    }

    /**
     * 如果当前不是NULL_PAGE，则表示往后还有更多的数据页
     */
    private int nextExtNum = -1;

    private int dataSize;

    @Override
    public synchronized void recover() throws IOException {
        if (stat != PageStat.FLUSHED && stat != PageStat.NEW) {
            return;
        }

        super.recover();

        nextExtNum = dataBuffer.unwrap().getInt();
        dataSize = dataBuffer.unwrap().getInt();
        stat = PageStat.RECOVERED_HEAD;
    }

    /**
     * 返回连接的下一个扩展页
     *
     * @return
     */
    public int nextExt() {
        return nextExtNum;
    }

    public void nextExt(int nextExtNum) {
        this.nextExtNum = nextExtNum;
    }

    /**
     * 获取扩展页的数据内容
     *
     * @return
     */
    public ByteBuffer getData() {
        dataBuffer.unwrap().position(8);
        ByteBuffer data = dataBuffer.unwrap().slice();
        return data.limit(dataSize);
    }

    /**
     * 写入数据内容到扩展页
     *
     * @param byteBuffer
     */
    public void putData(ByteBuffer byteBuffer) {
        dataSize = byteBuffer.limit();
        dataBuffer.unwrap().position(8);
        dataBuffer.unwrap().put(byteBuffer);
    }

    /**
     * 可存储的数据内容。
     *
     * @return
     */
    public int dataCapacity() {
        // 4字节的nextExt 4字节数据段大小
        return dataBuffer.unwrap().limit() - 4 - 4;
    }
}
