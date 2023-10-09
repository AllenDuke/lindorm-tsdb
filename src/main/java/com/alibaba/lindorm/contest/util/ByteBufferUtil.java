package com.alibaba.lindorm.contest.util;

import java.nio.ByteBuffer;

public class ByteBufferUtil {

    public static byte[] toBytes(ByteBuffer v) {
        byte[] array1 = null;
        if (v.hasArray()) {
            array1 = v.array();
            if (array1.length != v.remaining()) {
                array1 = null;
            }
        }
        if (array1 == null) {
            array1 = new byte[v.remaining()];
            v.get(array1);
        }
        return array1;
    }
}
