package com.alibaba.lindorm.contest.lsm;

public class TimeIndexItem {

    public static int SIZE = 16 + 8 + 4;

    private long minTime;

    private long maxTime;

    private long batchPos;

    private int batchSize;

    public TimeIndexItem(long minTime, long maxTime, long batchPos, int batchSize) {
        this.minTime = minTime;
        this.maxTime = maxTime;
        this.batchPos = batchPos;
        this.batchSize = batchSize;
    }

    public long getMinTime() {
        return minTime;
    }

    public long getMaxTime() {
        return maxTime;
    }

    public long getBatchPos() {
        return batchPos;
    }

    public int getBatchSize() {
        return batchSize;
    }
}
