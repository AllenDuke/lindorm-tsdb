package com.alibaba.lindorm.contest.lsm;

public class StringIndexItem {

    private int batchNum;

    private long pos;

    private int size;

    public StringIndexItem(int batchNum, long pos, int size) {
        this.batchNum = batchNum;
        this.pos = pos;
        this.size = size;
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
}
