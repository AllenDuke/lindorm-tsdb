package com.alibaba.lindorm.contest.storage;

import java.util.LinkedList;
import java.util.List;

public class WindowSearchRequest {

    private final long minTime;

    private final long maxTime;

    public WindowSearchRequest(long minTime, long maxTime) {
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
