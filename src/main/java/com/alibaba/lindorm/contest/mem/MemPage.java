package com.alibaba.lindorm.contest.mem;

import java.nio.*;

public class MemPage {

    private final ByteBuffer byteBuffer;

    public MemPage(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    /**
     * 不能作为某对象的属性
     *
     * @return
     */
    public ByteBuffer unwrap() {
        return byteBuffer;
    }
}
