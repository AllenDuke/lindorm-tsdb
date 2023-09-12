package com.alibaba.lindorm.contest.lsm;

public class TimeIndexItem {

    public static int SIZE = 16;

    private long minTime;

    private long maxTime;

    public TimeIndexItem(long minTime, long maxTime) {
        this.minTime = minTime;
        this.maxTime = maxTime;
    }

    public long getMinTime() {
        return minTime;
    }

    public long getMaxTime() {
        return maxTime;
    }
}
