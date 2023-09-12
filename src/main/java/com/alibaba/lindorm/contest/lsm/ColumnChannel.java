package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.structs.ColumnValue;

import java.io.*;
import java.util.List;

public abstract class ColumnChannel<C extends ColumnValue> {

    protected final File columnFile;

    protected final OutputStream columnOutput;

    public ColumnChannel(File vinDir, TableSchema.Column column) throws IOException {
        columnFile = new File(vinDir.getAbsolutePath(), column.columnName);
        if (!columnFile.exists()) {
            columnFile.createNewFile();
        }
        columnOutput = new BufferedOutputStream(new FileOutputStream(columnFile, true));
    }

    public abstract void append(C c) throws IOException;

    public abstract List<ColumnItem<C>> range(List<TimeItem> timeItemList) throws IOException;
