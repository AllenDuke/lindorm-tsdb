package com.alibaba.lindorm.contest.mem;

import com.alibaba.lindorm.contest.storage.AbPage;

import java.nio.ByteBuffer;
import java.util.*;

public class MemPagePool {

    /**
     * 小于16字节的buffer定义为Mini buffer
     */
    private static int MINI_BUFFER_SIZE = 16;

    /**
     * buffer池大小
     */
    private final int pageCapacity;

    private int pageAvailable;

    public MemPagePool(int pageCapacity) {
        this.pageCapacity = pageCapacity;
        pageAvailable = pageCapacity;
    }

    private Set<MemPage> free;

    private Set<MemPage> busy;

    /**
     * 迷你buffer共用一个大的。
     */
    private ByteBuffer miniBuffer;

    public synchronized MemPage allocate() {
        if (pageAvailable <= 0) {
            return null;
        }
        MemPage memPage;
        if (free != null && !free.isEmpty()) {
            memPage = free.stream().findFirst().get();
            free.remove(memPage);
        } else {
            free = new HashSet<>();
            ByteBuffer allocate = ByteBuffer.allocate((int) AbPage.PAGE_SIZE);
            memPage = new MemPage(allocate);
        }
        if (busy == null) {
            busy = new HashSet<>();
        }
        busy.add(memPage);
        pageAvailable--;
        return memPage;
    }

    public synchronized boolean free(MemPage memPage) {
        boolean remove = busy.remove(memPage);
        if (remove) {
            memPage.unwrap().clear();
            free.add(memPage);
            pageAvailable++;
        }
        return remove;
    }

    public int capacity() {
        return pageCapacity;
    }
}
