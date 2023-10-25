package com.alibaba.lindorm.contest.util;

import com.alibaba.lindorm.contest.elf.ElfOnChimpDecompressor;
import com.alibaba.lindorm.contest.elf.IDecompressor;
import com.alibaba.lindorm.contest.structs.ColumnValue;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.lindorm.contest.CommonUtils.ARRAY_BASE_OFFSET;
import static com.alibaba.lindorm.contest.CommonUtils.UNSAFE;

public class NumberUtil {

    public static List<Integer> rzInt(ByteBuffer buffer) throws IOException {
        List<Integer> ints = new ArrayList<>(buffer.limit() >> 2);
        int lastPre = buffer.getInt();
        int last = buffer.getInt();
        ints.add(lastPre);
        ints.add(last);
        while (buffer.hasRemaining()) {
            int cur = last - lastPre + last + zigZagDecode(readVInt(buffer));
            ints.add(cur);
            lastPre = last;
            last = cur;
        }
        return ints;
    }

    private static int readVInt(ByteBuffer buffer) throws IOException {
        byte b = buffer.get();
        if (b >= 0) return b;
        int i = b & 0x7F;
        b = buffer.get();
        i |= (b & 0x7F) << 7;
        if (b >= 0) return i;
        b = buffer.get();
        i |= (b & 0x7F) << 14;
        if (b >= 0) return i;
        b = buffer.get();
        i |= (b & 0x7F) << 21;
        if (b >= 0) return i;
        b = buffer.get();
        // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
        i |= (b & 0x0F) << 28;
        if ((b & 0xF0) == 0) return i;
        throw new IOException("Invalid vInt detected (too many bits)");
    }

    public static ByteBuffer zInt(List<Integer> ints) {
        ByteBuffer buffer = ByteBuffer.allocate(ints.size() * 5);
        int lastPre = ints.get(0);
        buffer.putInt(lastPre);
        int last = ints.get(1);
        buffer.putInt(last);
        for (int i = 2; i < ints.size(); i++) {
            int v = ints.get(i);
            v = zigZagEncode(ints.get(i)- last - (last - lastPre));
            while ((v & ~0x7F) != 0) {
                buffer.put((byte) ((v & 0x7F) | 0x80));
                v >>>= 7;
            }
            buffer.put((byte) v);
            lastPre = last;
            last = ints.get(i);
        }
        buffer.flip();
        return buffer;
    }

    public static int zigZagEncode(int i) {
        return (i >> 31) ^ (i << 1);
    }

    public static int zigZagDecode(int i) {
        return ((i >>> 1) ^ -(i & 1));
    }
}
