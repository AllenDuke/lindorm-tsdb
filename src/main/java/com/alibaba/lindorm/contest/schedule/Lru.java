package com.alibaba.lindorm.contest.schedule;

import com.alibaba.lindorm.contest.storage.AbPage;

import java.util.LinkedHashMap;
import java.util.Map;

class Lru<K, V extends AbPage> extends LinkedHashMap<K, V> {

    private final int maxCapacity;

    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    protected Lru(long maxCapacity) {
        super((int) maxCapacity, DEFAULT_LOAD_FACTOR, true);
        this.maxCapacity = (int) maxCapacity;
    }

    protected Map.Entry<K, V> removeOldest() {
        Map.Entry<K, V> entry = entrySet().stream().findFirst().get();
        remove(entry.getKey());
        return entry;
    }
}
