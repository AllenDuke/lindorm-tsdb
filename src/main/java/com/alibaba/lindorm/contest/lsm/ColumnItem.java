package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.structs.ColumnValue;

public class ColumnItem<I extends ColumnValue> {

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
