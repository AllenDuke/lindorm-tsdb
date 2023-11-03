package com.alibaba.lindorm.contest.lsm;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class ColumnIndexItem {

    private int batchNum;

    private long pos;

    private int size;

    private int batchItemCount;

    public ColumnIndexItem(int batchNum, long pos, int size, int batchItemCount) {
        this.batchNum = batchNum;
        this.pos = pos;
        this.size = size;
        this.batchItemCount = batchItemCount;
    }

    public int getBatchNum() {
        return batchNum;
    }

    public long getPos() {
        return pos;
    }

    public int getSize() {
        return size;
    }

    public abstract byte[] toBytes();

    public int getBatchItemCount() {
        return batchItemCount;
    }

    public abstract void write(ByteBuffer byteBuffer) throws IOException;
}
