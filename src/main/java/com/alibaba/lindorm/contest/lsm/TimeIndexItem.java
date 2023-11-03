package com.alibaba.lindorm.contest.lsm;

public class TimeIndexItem {

    public static int SIZE = 8 + 8 + 8 + 4;

    private long minTime;

    private long maxTime;

    private long pos;

    private int size;

    public long getMinTime() {
        return minTime;
    }

    public long getMaxTime() {
        return maxTime;
    }

    public TimeIndexItem(long minTime, long maxTime, long pos, int size) {
        this.minTime = minTime;
        this.maxTime = maxTime;
        this.pos = pos;
        this.size = size;
    }

    public long getPos() {
        return pos;
    }

    public int getSize() {
        return size;
    }
}
