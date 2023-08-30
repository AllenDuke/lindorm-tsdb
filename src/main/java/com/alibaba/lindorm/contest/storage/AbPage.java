package com.alibaba.lindorm.contest.storage;

import com.alibaba.lindorm.contest.mem.MemPagePool;
import com.alibaba.lindorm.contest.mem.MemPage;

import java.io.IOException;
import java.nio.channels.FileLock;

public abstract class AbPage {

    /**
     * 页大小16k
     */
    public static long PAGE_SIZE = 16 * 1024;

    /**
     * 表示往后没有更多的页
     */
    public static int NULL_PAGE = -1;

    /**
     * 页号
     */
    protected final int num;

    /**
     * 当前页所在文件
     */
    protected final VinStorage vinStorage;

    /**
     * flush会释放，
     */
    protected MemPage memPage;

    protected PageStat stat;

    public AbPage(VinStorage vinStorage, Integer num) {
        this.vinStorage = vinStorage;
        this.num = num;
        this.stat = PageStat.KEY;
    }

    public synchronized void map(MemPage memPage) {
        this.memPage = memPage;
    }

    public void recover() throws IOException {
        FileLock lock = vinStorage.dbChannel().lock(PAGE_SIZE * num, PAGE_SIZE, false);
        vinStorage.dbChannel().read(memPage.unwrap(), PAGE_SIZE * num);
        lock.release();

        // todo 最后一个buffer可能不是满的
//        if (memPage.unwrap().position() != memPage.unwrap().capacity()) {
//            throw new IllegalStateException("不是一个完整的buffer");
//        }

        memPage.unwrap().flip();
        stat = PageStat.RECOVERED;
    }

    /**
     * 数据刷盘
     */
    public void flush() throws IOException {
        memPage.unwrap().position(0);
        FileLock lock = vinStorage.dbChannel().lock(PAGE_SIZE * num, PAGE_SIZE, false);
        vinStorage.dbChannel().write(memPage.unwrap(), PAGE_SIZE * num);
        lock.release();

        stat = PageStat.FLUSHED;
    }

    @Override
    public int hashCode() {
        return vinStorage.vin().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        AbPage page = (AbPage) obj;
        return this.num == page.num && this.hashCode() == page.hashCode();
    }

    public VinStorage vinStorage() {
        return vinStorage;
    }

    public int pageNum() {
        return num;
    }

    public synchronized MemPage memPage() {
        return memPage;
    }
}
