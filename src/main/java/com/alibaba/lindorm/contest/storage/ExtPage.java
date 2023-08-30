package com.alibaba.lindorm.contest.storage;

import com.alibaba.lindorm.contest.mem.MemPagePool;

import java.io.IOException;
import java.nio.ByteBuffer;

class ExtPage extends AbPage {

    public ExtPage(VinStorage vinStorage, Integer num) {
        super(vinStorage, num);
    }

    /**
     * 如果当前不是NULL_PAGE，则表示往后还有更多的数据页
     */
    private int nextExtNum = -1;

    private int dataSize;

    @Override
    public synchronized void recover() throws IOException {
        super.recover();

        nextExtNum = memPage.unwrap().getInt();
        dataSize = memPage.unwrap().getInt();
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

        memPage.unwrap().position(0);
        memPage.unwrap().putInt(nextExtNum);
    }

    /**
     * 获取扩展页的数据内容
     *
     * @return
     */
    public ByteBuffer getData() {
        memPage.unwrap().position(8);
        ByteBuffer data = memPage.unwrap().slice();
        return data.limit(dataSize);
    }

    /**
     * 写入数据内容到扩展页
     *
     * @param byteBuffer
     */
    public void putData(ByteBuffer byteBuffer) {
        dataSize = byteBuffer.limit();
        memPage.unwrap().position(4);
        memPage.unwrap().putInt(dataSize);
        memPage.unwrap().put(byteBuffer);
    }

    /**
     * 可存储的数据内容。
     *
     * @return
     */
    public int dataCapacity() {
        // 4字节的nextExt 4字节数据段大小
        return memPage.unwrap().limit() - 4 - 4;
    }
}
