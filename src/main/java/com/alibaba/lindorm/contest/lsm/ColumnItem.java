package com.alibaba.lindorm.contest.lsm;

public class ColumnItem<I> {

    private I item;

    private long itemNum;

    public ColumnItem(I item, long itemNum) {
        this.item = item;
        this.itemNum = itemNum;
    }

    public I getItem() {
        return item;
    }

    public long getItemNum() {
        return itemNum;
    }
}
