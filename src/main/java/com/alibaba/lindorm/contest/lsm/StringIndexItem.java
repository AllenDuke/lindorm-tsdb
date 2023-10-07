package com.alibaba.lindorm.contest.lsm;

import java.nio.ByteBuffer;

public class StringIndexItem extends ColumnIndexItem {

    public StringIndexItem(int batchNum, long pos, int size) {
        super(batchNum, pos, size);
    }

    @Override
    public byte[] toBytes() {
        ByteBuffer allocate = ByteBuffer.allocate(8 + 4);
        allocate.putLong(getPos());
        allocate.putInt(getSize());
        return allocate.array();
    }
}
