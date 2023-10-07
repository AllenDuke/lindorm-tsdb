package com.alibaba.lindorm.contest.lsm;

import java.nio.ByteBuffer;

public class IntIndexItem extends ColumnIndexItem {

    private long batchSum;

    private int batchMax;

    public IntIndexItem(int batchNum, long pos, int size, long batchSum, int batchMax) {
        super(batchNum, pos, size);
        this.batchSum = batchSum;
        this.batchMax = batchMax;
    }

    public long getBatchSum() {
        return batchSum;
    }

    public int getBatchMax() {
        return batchMax;
    }

    @Override
    public byte[] toBytes() {
        ByteBuffer allocate = ByteBuffer.allocate(8 + 4 + 8 + 4);
        allocate.putLong(batchSum);
        allocate.putInt(batchMax);
        allocate.putLong(getPos());
        allocate.putInt(getSize());
        return allocate.array();
    }
}
