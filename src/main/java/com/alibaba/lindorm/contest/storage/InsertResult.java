package com.alibaba.lindorm.contest.storage;

import com.alibaba.lindorm.contest.structs.Row;

import java.util.List;

public class InsertResult {

    /**
     * -1表示不需要往左检索
     */
    private int nextLeft = -1;

    /**
     * -1表示不需要往右检索
     */
    private int nextRight = -1;

    private List<Row> transfer;

    private boolean inserted = false;

    public int getNextLeft() {
        return nextLeft;
    }

    public void setNextLeft(int nextLeft) {
        this.nextLeft = nextLeft;
    }

    public int getNextRight() {
        return nextRight;
    }

    public void setNextRight(int nextRight) {
        this.nextRight = nextRight;
    }

    public boolean isInserted() {
        return inserted;
    }

    public void setInserted(boolean inserted) {
        this.inserted = inserted;
    }

    public List<Row> getTransfer() {
        return transfer;
    }

    public void setTransfer(List<Row> transfer) {
        this.transfer = transfer;
    }
}
