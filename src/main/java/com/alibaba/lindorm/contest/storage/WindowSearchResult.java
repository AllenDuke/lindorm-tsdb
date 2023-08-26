package com.alibaba.lindorm.contest.storage;

import com.alibaba.lindorm.contest.structs.Row;

import java.util.List;

public class WindowSearchResult {

    /**
     * -1表示不需要往左检索
     */
    private int nextLeft = -1;

    /**
     * -1表示不需要往右检索
     */
    private int nextRight = -1;

    /**
     * 当前检索页号
     */
    private final int num;

    private List<Row> rowList;

    public WindowSearchResult(int num) {
        this.num = num;
    }

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

    public int getNum() {
        return num;
    }

    public List<Row> getRowList() {
        return rowList;
    }

    public void setRowList(List<Row> rowList) {
        this.rowList = rowList;
    }
}
