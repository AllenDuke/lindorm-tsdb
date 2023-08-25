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

    private Set<PooledByteBuffer> free;

    private Set<PooledByteBuffer> busy;

    /**
     * 迷你buffer共用一个大的。
     */
    private ByteBuffer miniBuffer;

    public synchronized PooledByteBuffer allocate(int need) {
        if (need > available) {
            return null;
        }
        PooledByteBuffer pooledByteBuffer;
        if (free != null && !free.isEmpty()) {
            pooledByteBuffer = free.stream().findFirst().get();
            free.remove(pooledByteBuffer);
        } else {
            free = new HashSet<>();
            ByteBuffer allocate = ByteBuffer.allocate(need);
            pooledByteBuffer = new PooledByteBuffer(allocate);
        }
        if (busy == null) {
            busy = new HashSet<>();
        }
        busy.add(pooledByteBuffer);
        available -= need;
        return pooledByteBuffer;
    }

    public synchronized boolean free(PooledByteBuffer buffer) {
        boolean remove = busy.remove(buffer);
        if (remove) {
            buffer.unwrap().clear();
            free.add(buffer);
        }
        return remove;
    }
}
