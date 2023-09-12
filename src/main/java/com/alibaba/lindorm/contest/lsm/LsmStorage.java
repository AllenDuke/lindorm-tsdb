package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LsmStorage {

    /**
     * 每8k数据为一块
     */
    public static final int MAX_ITEM_CNT_L0 = 8 * 1024;

    private final File dir;

    private final Vin vin;

    private final TableSchema tableSchema;

    /**
     * 数据文件
     */
    private final Map<TableSchema.Column, OutputStream> dataOutputMap = new HashMap<>();

    private final TimeChannel timeChannel;

    public LsmStorage(File dbDir, Vin vin, TableSchema tableSchema) throws IOException {
        String vinStr = new String(vin.getVin(), StandardCharsets.UTF_8);
        this.dir = new File(dbDir.getAbsolutePath(), vinStr);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        this.vin = vin;
        this.tableSchema = tableSchema;
        this.timeChannel = new TimeChannel(dir);

        for (TableSchema.Column column : tableSchema.getColumnList()) {

        }
    }

}
