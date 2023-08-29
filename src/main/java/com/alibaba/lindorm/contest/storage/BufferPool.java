package com.alibaba.lindorm.contest.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class BufferPool {

    /**
     * 小于16字节的buffer定义为Mini buffer
     */
    private static int MINI_BUFFER_SIZE = 16;

    /**
     * buffer池大小
     */
    private final long size;

    private volatile long available;

    private final List<VinStorage> vinStorageList;

    private Thread scheduler;

    public BufferPool(long size) {
        this.size = size;
        available = size;

        vinStorageList = new ArrayList<>();

        scheduler = new Thread(() -> {
            while (true) {
                while (available < size * 0.5) {
                    schedule();
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "buffer-scheduler");
        scheduler.setDaemon(true);
//        scheduler.start();
    }

    private Set<PooledByteBuffer> free;

    private Set<PooledByteBuffer> busy;

    /**
     * 迷你buffer共用一个大的。
     */
    private ByteBuffer miniBuffer;

    private int lastScheduleVinStorage = -1;

    public void register(VinStorage vinStorage) {
        vinStorageList.add(vinStorage);
    }

    private synchronized void schedule() {
        lastScheduleVinStorage++;
        if (lastScheduleVinStorage == vinStorageList.size()) {
            lastScheduleVinStorage = 0;
        }
        if (vinStorageList.isEmpty()) {
            return;
        }
        try {
            vinStorageList.get(lastScheduleVinStorage).flushOldPage();
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException("调度刷盘异常");
        }
    }

    public synchronized PooledByteBuffer allocate() {
        int need = (int) AbPage.PAGE_SIZE;
        while (need > available) {
            // 申请失败 当前线程帮助调度
            schedule();
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
            available += buffer.unwrap().capacity();
        }
        return remove;
    }
}
