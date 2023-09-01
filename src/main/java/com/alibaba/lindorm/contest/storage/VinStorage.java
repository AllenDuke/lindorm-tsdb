package com.alibaba.lindorm.contest.storage;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.schedule.PageScheduler;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.*;
import java.lang.reflect.Constructor;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

public class VinStorage {

    private final Vin vin;

    /**
     * 最大页号页，因为插入序列是时序有关的，所以可以认为超90%的数据序列本事是时间升序的，所以每次插入/查询时，从最大页号页开始遍历。
     * 创建新页时，自动更新
     */
    private int maxTimeSortedPage = -1;

    private File dbFile;

    private File indexFile;

    private FileChannel dbChannel;

    private final AtomicInteger pageCount = new AtomicInteger(0);

    private final String path;

    private final ArrayList<String> columnNameList;

    private final ArrayList<ColumnValue.ColumnType> columnTypeList;

    /**
     * 保存最新的
     */
    private long latestRowKey = -1;

    private boolean connected = false;

    /**
     * 单个vin文件串行读写
     */
    private final Lock vinLock = new ReentrantLock();

    /**
     * 用于对该vin映射的内存页进行调度
     */
    private final Lock scheduleLock = new ReentrantLock();

    /**
     * 最大栈深为2
     */
    private final Stack<TimeSortedPage> pageStack = new Stack<>();

    private final Map<TimeSortedPage, Thread> pageWaiterMap = new ConcurrentHashMap<>();

    public VinStorage(Vin vin, String path, ArrayList<String> columnNameList, ArrayList<ColumnValue.ColumnType> columnTypeList) {
        this.vin = vin;
        this.path = path;
        this.columnNameList = columnNameList;
        this.columnTypeList = columnTypeList;
    }

    private void init() throws IOException {
        if (connected) {
            return;
        }
        String vinStr = new String(vin.getVin(), StandardCharsets.UTF_8);
        dbFile = new File(path, vinStr + ".db");
        indexFile = new File(path, vinStr + ".idx");
        if (!dbFile.exists()) {
            dbFile.createNewFile();
        }
        if (!indexFile.exists()) {
            indexFile.createNewFile();
        }
        dbChannel = new RandomAccessFile(dbFile, "rw").getChannel();
        if (dbChannel.size() > 0) {
            // 从文件中恢复
            FileInputStream inputStream = new FileInputStream(indexFile);
            pageCount.set(CommonUtils.readInt(inputStream));
            maxTimeSortedPage = CommonUtils.readInt(inputStream);
            latestRowKey = CommonUtils.readLong(inputStream);
            inputStream.close();
        }

        connected = true;
    }

    public ArrayList<Row> window(long minTime, long maxTime) throws IOException {
        vinLock.lock();
        init();

        if (maxTimeSortedPage == -1) {
            return new ArrayList<>(0);
        }

        WindowSearchRequest request = new WindowSearchRequest(minTime, maxTime);
        ArrayList<Row> rows = new ArrayList<>();

        TimeSortedPage cur;

        scheduleLock.lock();
        pageStack.push(getPage(TimeSortedPage.class, maxTimeSortedPage));
        scheduleLock.unlock();

        while (!pageStack.isEmpty()) {
            cur = pageStack.peek();
            WindowSearchResult result = cur.search(request);
            List<Row> rowList = result.getRowList();
            if (rowList != null && !rowList.isEmpty()) {
                rows.addAll(rowList);
            }

            scheduleLock.lock();
            pageStack.pop();
            Thread waiter = pageWaiterMap.remove(cur);

            int nextLeft = result.getNextLeft();
            if (nextLeft != -1) {
                pageStack.push(getPage(TimeSortedPage.class, nextLeft));
            }
            int nextRight = result.getNextRight();
            if (nextRight != -1) {
                pageStack.push(getPage(TimeSortedPage.class, nextRight));
            }

            scheduleLock.unlock();
            if (waiter != null) {
                LockSupport.unpark(waiter);
            }
        }
        vinLock.unlock();
        return rows;
    }

    public Row latest() throws IOException {
        init();
        ArrayList<Row> window = window(latestRowKey, latestRowKey + 1);
        if (window.isEmpty()) {
            return null;
        }
        return window.get(0);
    }

    public boolean insert(Row row) throws IOException {
        vinLock.lock();

        Vin vin = row.getVin();
        if (!this.vin.equals(vin)) {
            return false;
        }

        init();

        if (latestRowKey == -1 || row.getTimestamp() >= latestRowKey) {
            latestRowKey = row.getTimestamp();
        }

        /**
         * 从最大节点开始寻找 插入
         */
        TimeSortedPage cur;

        scheduleLock.lock();
        if (maxTimeSortedPage == -1) {
            cur = creatPage(TimeSortedPage.class);
            maxTimeSortedPage = cur.num;
        } else {
            cur = getPage(TimeSortedPage.class, maxTimeSortedPage);
        }
        pageStack.push(cur);
        scheduleLock.unlock();

        int nextTry = -1;
        while ((nextTry = cur.insert(row.getTimestamp(), row)) != cur.num) {
            if (nextTry != -1) {
                cur = getPage(TimeSortedPage.class, nextTry);
                continue;
            }

            // 在当前的左边申请新的一页插入
            TimeSortedPage left = creatPage(TimeSortedPage.class);
            // insert前调整链表，因为insert可能会导致flush
            left.connectRightBeforeFlushingByForce(cur);
            left.insert(row.getTimestamp(), row);
            break;
        }
        vinLock.unlock();
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

    private <P extends AbPage> P newPage(Class<P> pClass, int newPageNum) {
        if (newPageNum == -1) {
            throw new IllegalStateException("未识别的页号");
        }

        P page = null;
        try {
            Constructor<P> constructor = pClass.getConstructor(VinStorage.class, Integer.class);
            page = constructor.newInstance(this, newPageNum);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return page;
    }

    private void updateMaxPage(TimeSortedPage page) {
        this.maxTimeSortedPage = page.pageNum();
    }

    /**
     * 在文件中开辟新的一页
     *
     * @param pClass
     * @param <P>
     * @return
     */
    protected <P extends AbPage> P creatPage(Class<P> pClass) {
        int newPageNum = pageCount.getAndIncrement();
        P page = newPage(pClass, newPageNum);
        if (page instanceof TimeSortedPage) {
            updateMaxPage((TimeSortedPage) page);
        }
        do {
            /**
             * 当调度成功，映射到内存页后，才尝试对自身进行锁定。锁定成功后才往下操作。
             *
             * 不首先锁定自身，是因为schedule evict也是同步区域，防止a在等待schedule，而b在等待a页evict而造成死锁。
             *
             * while循环是为了防止映射到内存页后有立马被调度需要过期。
             */
            page = (P) PageScheduler.PAGE_SCHEDULER.schedule(page);
        } while (scheduleLock.tryLock());
        return page;
    }

    protected <P extends AbPage> P getPage(Class<P> pClass, int pageNum) {
        if (connected && (pageNum < 0 || pageNum >= pageCount.get())) {
            return null;
        }
        P pageKey = newPage(pClass, pageNum);
        AbPage page;
        do {
            page = PageScheduler.PAGE_SCHEDULER.schedule(pageKey);
        } while (scheduleLock.tryLock());
        if (page != pageKey) {
            // 该页还没有换出
            return (P) page;
        }
        // 该页已经在内存中释放，从文件中恢复
        try {
            page.recover();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException(pageNum + "号页恢复异常");
        }
        return (P) page;
    }

    protected ArrayList<String> columnNameList() {
        return columnNameList;
    }

    protected ArrayList<ColumnValue.ColumnType> columnTypeList() {
        return columnTypeList;
    }

    public void shutdown() throws IOException {
        vinLock.lock();
        if (!connected) {
            return;
        }

        FileOutputStream outputStream = new FileOutputStream(indexFile);
        CommonUtils.writeInt(outputStream, pageCount.get());
        CommonUtils.writeInt(outputStream, maxTimeSortedPage);
        CommonUtils.writeLong(outputStream, latestRowKey);
        outputStream.flush();
        outputStream.close();
        dbChannel.close();
        connected = false;
        vinLock.unlock();
    }

    public Vin vin() {
        return vin;
    }

    /**
     * 页过期，由调度器调用
     *
     * @param page
     */
    public void evict(AbPage page) {
        boolean permit = true;
        while (true) {
            scheduleLock.lock();
            for (TimeSortedPage usingPage : pageStack) {
                if (usingPage == page) {
                    permit = false;

                    pageWaiterMap.put(usingPage, Thread.currentThread());
                    scheduleLock.unlock();
                    LockSupport.park();
                    break;
                }
            }
            if (!permit) {
                continue;
            }
            try {
                page.flush();
            } catch (Exception e) {
                e.printStackTrace();
                throw new IllegalStateException("页刷盘异常");
            }
            break;
        }
        scheduleLock.unlock();
    }
}
