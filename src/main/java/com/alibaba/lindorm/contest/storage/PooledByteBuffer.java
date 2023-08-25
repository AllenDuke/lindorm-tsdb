package com.alibaba.lindorm.contest.storage;

import java.nio.*;

public class PooledByteBuffer {

    private final ByteBuffer byteBuffer;

    PooledByteBuffer(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    /**
     * 不能作为某对象的属性
     * @return
     */
    public ByteBuffer unwrap() {
        return byteBuffer;
    }

}
