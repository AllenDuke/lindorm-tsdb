package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.TSDBEngineImpl;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.File;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class LsmStorage {

    /**
     * l0每个文件放8k条数据。
     */
    private static final int MAX_ITEM_CNT_L0 = 8 * 1024;

    /**
     * 0层file数量。
     */
    private static final byte FILE_CNT_L0 = 4;

    private final File dir;

    private final Vin vin;

    private final List<String> columnsName;

    private SsTable memTable;

    private SsTable frozenTable;

    /**
     * 每一层的file数量。
     */
    private final byte[] levelFileCnt = new byte[4];

    private byte l0FilePos = 0;

    public LsmStorage(File dbDir, Vin vin, List<String> columnsName) {
        String vinStr = new String(vin.getVin(), StandardCharsets.UTF_8);
        this.dir = new File(dbDir.getAbsolutePath(), vinStr);
        this.vin = vin;
        this.columnsName = columnsName;
    }

    public void put(Row row) {
        if (memTable == null) {

        }
    }
}
