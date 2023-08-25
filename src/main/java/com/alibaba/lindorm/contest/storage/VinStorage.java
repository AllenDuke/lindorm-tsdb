package com.alibaba.lindorm.contest.storage;

import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class VinStorage {

    private static BufferPool COMMON_POOL = new BufferPool((long) (Runtime.getRuntime().totalMemory() * 0.6));

    private final Vin vin;

    private TimeSortedPage root;

    private FileChannel dbChannel;

    private int pageCount = 0;

    public VinStorage(Vin vin) {
        this.vin = vin;
    }

    private void init() throws IOException {
        String vinStr = new String(vin.getVin(), StandardCharsets.UTF_8);
        File dbFile = new File(vinStr + ".db");
        if (!dbFile.exists()) {
            dbFile.createNewFile();
        }
        dbChannel = new FileInputStream(dbFile).getChannel();
        root = new TimeSortedPage(this, COMMON_POOL, 0);
        pageCount++;
    }

    public synchronized boolean insert(Row row) throws IOException {
        Vin vin = row.getVin();
        if (!this.vin.equals(vin)) {
            return false;
        }

        if (dbChannel == null) {
            init();
        }

        root.insert(row.getTimestamp(), row);
        return true;
    }

    public long size() {
        return pageCount * AbPage.PAGE_SIZE;
    }

    public FileChannel dbChannel() {
        return dbChannel;
    }
}
