package com.alibaba.lindorm.contest.lsm;

public class TimeItem {

    /**
     * agg时可能不会设值
     */
    private long time;

    /**
     * 行号
     */
    private long itemNum;

    public TimeItem(long time, long itemNum) {
        this.time = time;
        this.itemNum = itemNum;
    }

    public long getTime() {
        return time;
    }

    public long getItemNum() {
        return itemNum;
    }

    public long getBatchNum() {
        return itemNum / LsmStorage.MAX_ITEM_CNT_L0;
    }
}
