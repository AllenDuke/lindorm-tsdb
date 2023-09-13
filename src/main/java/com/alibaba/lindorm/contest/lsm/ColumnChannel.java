package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.structs.ColumnValue;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.List;

public abstract class ColumnChannel<C extends ColumnValue> {

    protected final File columnFile;

    protected final OutputStream columnOutput;

    protected final FileChannel columnInput;

    public ColumnChannel(File vinDir, TableSchema.Column column) throws IOException {
        columnFile = new File(vinDir.getAbsolutePath(), column.columnName);
        if (!columnFile.exists()) {
            columnFile.createNewFile();
        }
        columnOutput = new BufferedOutputStream(new FileOutputStream(columnFile, true));
        columnInput = new RandomAccessFile(columnFile, "r").getChannel();
    }

    public abstract void append(C c) throws IOException;

    public abstract List<ColumnItem<C>> range(List<TimeItem> timeItemList) throws IOException;

    public void shutdown() throws IOException {
        columnOutput.flush();
        columnOutput.close();
        columnInput.close();
    }
}