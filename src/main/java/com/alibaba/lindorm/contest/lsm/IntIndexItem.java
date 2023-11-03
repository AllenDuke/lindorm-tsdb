package com.alibaba.lindorm.contest.lsm;

import java.io.IOException;
import java.nio.ByteBuffer;

public class IntIndexItem extends ColumnIndexItem {

    private long batchSum;

    private int batchMax;

    public IntIndexItem(int batchNum, long pos, int size, long batchSum, int batchMax, int batchItemCount) {
        super(batchNum, pos, size, batchItemCount);
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

    @Override
    public void write(ByteBuffer byteBuffer) throws IOException {
        byteBuffer.putLong(batchSum);
        byteBuffer.putInt(batchMax);
        byteBuffer.putLong(getPos());
        byteBuffer.putInt(getSize());
        byteBuffer.putInt(getBatchItemCount());
    }
}
