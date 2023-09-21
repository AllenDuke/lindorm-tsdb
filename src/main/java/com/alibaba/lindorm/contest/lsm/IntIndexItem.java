package com.alibaba.lindorm.contest.lsm;

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
}
