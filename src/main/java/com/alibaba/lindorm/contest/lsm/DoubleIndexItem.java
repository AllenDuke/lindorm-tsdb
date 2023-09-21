package com.alibaba.lindorm.contest.lsm;

public class DoubleIndexItem extends ColumnIndexItem {

    private double batchSum;

    private double batchMax;

    public DoubleIndexItem(int batchNum, long pos, int size, double batchSum, double batchMax) {
        super(batchNum, pos, size);
        this.batchSum = batchSum;
        this.batchMax = batchMax;
    }

    public double getBatchSum() {
        return batchSum;
    }

    public double getBatchMax() {
        return batchMax;
    }
}
