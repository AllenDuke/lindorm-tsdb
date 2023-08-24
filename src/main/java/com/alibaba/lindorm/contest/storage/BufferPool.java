package com.alibaba.lindorm.contest.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BufferPool {

    /**
     * 小于16字节的buffer定义为Mini buffer
     */
    private static int MINI_BUFFER_SIZE = 16;

    /**
     * buffer池大小
     */
    private final long size;

    private long available;

    public BufferPool(long size) {
        this.size = size;
        available = size;
    }

    private Set<ByteBuffer> free;

    private Set<ByteBuffer> busy;

    /**
     * 迷你buffer共用一个大的。
     */
    private ByteBuffer miniBuffer;

    public synchronized ByteBuffer allocate(int need) {
        if (need > available) {
            return null;
        }
        ByteBuffer allocate;
        if (free != null && !free.isEmpty()) {
            allocate = free.stream().findFirst().get();
            free.remove(allocate);
        } else {
            free = new HashSet<>();
            allocate = ByteBuffer.allocate(need);
        }
        if (busy == null) {
            busy = new HashSet<>();
        }
        busy.add(allocate);
        available -= need;
        return allocate;
    }

    public synchronized boolean free(ByteBuffer buffer) {
        buffer.equals()
        boolean remove = busy.remove(buffer);
        if (remove) {
            free.add(buffer);
        }
        return remove;
    }
}
