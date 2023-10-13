package com.alibaba.lindorm.contest.lsm;

import java.nio.ByteBuffer;

public class DoubleIndexItem extends ColumnIndexItem {

    private double batchSum;

    private double batchMax;

    private int batchMaxScale;

    public DoubleIndexItem(int batchNum, long pos, int size, double batchSum, double batchMax, int batchMaxScale) {
        super(batchNum, pos, size);
        this.batchSum = batchSum;
        this.batchMax = batchMax;
        this.batchMaxScale = batchMaxScale;
    }

    public double getBatchSum() {
        return batchSum;
    }

    public double getBatchMax() {
        return batchMax;
    }

    @Override
    public byte[] toBytes() {
        ByteBuffer allocate = ByteBuffer.allocate(8 + 8 + 8 + 4);
        allocate.putDouble(batchSum);
        allocate.putDouble(batchMax);
        allocate.putLong(getPos());
        allocate.putInt(getSize());
        return allocate.array();
    }
}
