package com.alibaba.lindorm.contest.schedule;

import com.alibaba.lindorm.contest.mem.MemPage;
import com.alibaba.lindorm.contest.mem.MemPagePool;
import com.alibaba.lindorm.contest.storage.AbPage;
import com.alibaba.lindorm.contest.storage.PageStat;

import java.io.IOException;
import java.util.Map;

public class PageScheduler {

    /**
     * 内存10%用来做内存页
     */
    private static final MemPagePool MEM_PAGE_POOL = new MemPagePool((int) ((Runtime.getRuntime().totalMemory() * 0.1) / AbPage.PAGE_SIZE));

    public static final PageScheduler PAGE_SCHEDULER = new PageScheduler(MEM_PAGE_POOL.capacity());

    static {
        System.out.println("jvm内存大小：" + Runtime.getRuntime().totalMemory() / 1024 / 1024 + "MB");
        System.out.println("PAGE_SCHEDULER管理页数：" + PAGE_SCHEDULER.capacity() + "页");
    }

    private final int capacity;

    private final Lru<AbPage, AbPage> lru;

    private boolean startLru = false;

    public PageScheduler(int capacity) {
        this.capacity = capacity;
        this.lru = new Lru<>(capacity);
    }

    public AbPage schedule(AbPage unMapPage) {
        Map.Entry<AbPage, AbPage> oldest;
        MemPage memPage;
        synchronized (this) {
            AbPage mapAbPage = lru.get(unMapPage);
            if (mapAbPage != null) {
                return mapAbPage;
            }

            if (lru.size() < capacity) {
                memPage = MEM_PAGE_POOL.allocate();
                unMapPage.map(memPage);
                lru.put(unMapPage, unMapPage);
                return unMapPage;
            }
            // 移除最老元素，防止多个线程争抢同一页，调整映射关系
            oldest = lru.removeOldest();
            // lru调整完毕，释放当前锁，防止死锁
            if (!startLru) {
                System.out.println("开始lru调度");
                startLru = true;
            }
        }

        AbPage page = oldest.getValue();
        // 通知过期、及时刷盘
        page.vinStorage().evict(page);
        memPage = page.memPage();
        memPage.unwrap().clear();
        unMapPage.map(memPage);

        // 调整完毕，加入调度
        synchronized (this) {
            lru.put(unMapPage, unMapPage);
        }
        return unMapPage;
    }

    public synchronized void shutdown() {
        System.out.println("shutdown所有页刷盘");
        for (AbPage page : lru.values()) {
            page.vinStorage().evict(page);
        }
        lru.clear();
        System.out.println("shutdown所有页刷盘完成");
    }

    public int capacity() {
        return capacity;
    }
}
