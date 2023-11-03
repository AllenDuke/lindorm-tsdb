package com.alibaba.lindorm.contest.lsm;

import java.io.IOException;
import java.nio.ByteBuffer;

public class StringIndexItem extends ColumnIndexItem {

    public StringIndexItem(int batchNum, long pos, int size, int batchItemCount) {
        super(batchNum, pos, size, batchItemCount);
    }

    @Override
    public byte[] toBytes() {
        ByteBuffer allocate = ByteBuffer.allocate(8 + 4);
        allocate.putLong(getPos());
        allocate.putInt(getSize());
        return allocate.array();
    }

    @Override
    public void write(ByteBuffer byteBuffer) throws IOException {
        byteBuffer.putLong(getPos());
        byteBuffer.putInt(getSize());
        byteBuffer.putInt(getBatchItemCount());
    }
}
