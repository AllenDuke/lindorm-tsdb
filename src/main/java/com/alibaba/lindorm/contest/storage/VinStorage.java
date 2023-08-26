package com.alibaba.lindorm.contest.storage;

import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class VinStorage {

    /**
     * 内存的60%用来做内存页
     */
    private static BufferPool COMMON_POOL = new BufferPool((long) (Runtime.getRuntime().totalMemory() * 0.6));

    private final Vin vin;

    /**
     * 最大页号页，因为插入序列是时序有关的，所以可以认为超90%的数据序列本事是时间升序的，所以每次插入/查询时，从最大页号页开始遍历。
     * 创建新页时，自动更新
     */
    private TimeSortedPage maxPage;

    private FileChannel dbChannel;

    private AtomicInteger pageCount = new AtomicInteger(0);

    private final String path;

    private final List<String> columnNameList;

    /**
     * todo 释放内存
     */
    private final Map<Integer, AbPage> pageMap = new ConcurrentHashMap<>();

    public VinStorage(Vin vin, String path, List<String> columnNameList) {
        this.vin = vin;
        this.path = path;
        this.columnNameList = columnNameList;
    }

    private void init() throws IOException {
        String vinStr = new String(vin.getVin(), StandardCharsets.UTF_8);
        File dbFile = new File(path, vinStr + ".db");
        if (!dbFile.exists()) {
            dbFile.createNewFile();
        }
        dbChannel = new FileInputStream(dbFile).getChannel();
        creatPage(TimeSortedPage.class);
    }

    public boolean insert(Row row) throws IOException {
        synchronized (this) {
            Vin vin = row.getVin();
            if (!this.vin.equals(vin)) {
                return false;
            }

            if (dbChannel == null) {
                init();
            }
        }

        /**
         * 从根节点开始寻找 插入
         */
        TimeSortedPage cur = maxPage;
        int nextTry = -1;
        while ((nextTry = cur.insert(row.getTimestamp(), row)) != cur.num) {
            if (nextTry != -1) {
                cur = getPage(TimeSortedPage.class, nextTry);
                continue;
            }
            // 申请新的一页插入
            TimeSortedPage next = creatPage(TimeSortedPage.class);
            next.insert(row.getTimestamp(), row);

            // 调整链表
            cur.connect(next);
        }
        return true;
    }

    /**
     * 文件大小
     *
     * @return
     */
    public long size() {
        return pageCount.get() * AbPage.PAGE_SIZE;
    }

    public FileChannel dbChannel() {
        return dbChannel;
    }

    public <P extends AbPage> P newPage(Class<P> pClass, int newPageNum) {
        if (newPageNum == -1) {
            throw new IllegalStateException("未识别的页号");
        }

        P page = null;
        try {
            Constructor<P> constructor = pClass.getConstructor(VinStorage.class, BufferPool.class, Integer.class);
            page = constructor.newInstance(this, COMMON_POOL, newPageNum);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return page;
    }

    private synchronized void updateMaxPage(TimeSortedPage page) {
        this.maxPage = page;
    }

    public <P extends AbPage> P creatPage(Class<P> pClass) {
        int newPageNum = pageCount.incrementAndGet();
        P page = newPage(pClass, newPageNum);
        pageMap.put(newPageNum, page);
        if (page instanceof TimeSortedPage) {
            updateMaxPage((TimeSortedPage) page);
        }
        return page;
    }

    public <P extends AbPage> P getPage(Class<P> pClass, int pageNum) {
        if (pageNum >= pageCount.get()) {
            return null;
        }
        AbPage page = pageMap.computeIfAbsent(pageNum, k -> {
            // 该页已经在内存中释放，从文件中恢复
            AbPage p = newPage(pClass, pageNum);
            try {
                p.recover();
            } catch (IOException e) {
                e.printStackTrace();
                throw new IllegalStateException(pageNum + "号页恢复异常");
            }
            return p;
        });
        return (P) page;
    }

    public List<String> schema() {
        return columnNameList;
    }
}
