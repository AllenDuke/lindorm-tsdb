package com.alibaba.lindorm.contest.util;

import java.util.ArrayList;
import java.util.List;

public class NumberUtil {

    /**
     * 单个整型的变长化递归实现
     *
     * @param value
     * @param iv
     */
    protected static void toByte(List<Byte> value, int iv) {
        byte i = (byte) (iv & 0x7f);
        iv = iv >> 7;
        if (iv > 0) {
            i = (byte) (i | 0x80);
            value.add(i);
            toByte(value, iv);
        } else {
            value.add(i);
        }
    }

    /**
     * 将变长数组转化为int数组
     *
     * @param bytes
     * @return
     */
    public static int[] toInt(byte[] bytes) {
        ArrayList<Integer> array = new ArrayList<Integer>();
        int temp = 0;
        int index = 0;
        for (byte b : bytes) {
            if ((b & 0x80) == 0) {
                temp |= b << (index * 7);
                array.add(temp);
                index = 0;
                temp = 0;
            } else {
                temp |= (b & 0x7f) << (index++ * 7);
            }
        }
        int[] ints = new int[array.size()];
        for (int i = 0; i < array.size(); i++) {
            ints[i] = array.get(i);
        }
        return ints;
    }

    public static int zigZagEncode(int i) {
        return (i >> 31) ^ (i << 1);
    }

    public static int zigZagDecode(int i) {
        return ((i >>> 1) ^ -(i & 1));
    }
}
