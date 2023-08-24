package com.alibaba.lindorm.contest.storage;

import java.nio.*;

public class PooledByteBuffer {

    private final ByteBuffer byteBuffer;

    private final int num;

    PooledByteBuffer(ByteBuffer byteBuffer, int num) {
        this.byteBuffer = byteBuffer;
        this.num = num;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public int getNum() {
        return num;
    }
}
