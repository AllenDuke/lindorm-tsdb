package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.structs.Aggregator;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ColumnChannel<C extends ColumnValue> {

    protected final File columnFile;

    protected final OutputStream columnOutput;

    /**
     * todo lru
     */
    private final RandomAccessFile columnInput;

    public ColumnChannel(File vinDir, TableSchema.Column column) throws IOException {
        columnFile = new File(vinDir.getAbsolutePath(), column.columnName);
        if (!columnFile.exists()) {
            columnFile.createNewFile();
        }
        columnOutput = new BufferedOutputStream(new FileOutputStream(columnFile, true));
        columnInput = new RandomAccessFile(columnFile, "r");
    }

    public abstract void append(C c) throws IOException;

    public abstract List<ColumnItem<C>> range(List<TimeItem> timeItemList) throws IOException;

    public abstract ColumnValue agg(List<TimeItem> timeItemList, Aggregator aggregator, CompareExpression columnFilter) throws IOException;

    public void shutdown() throws IOException {
        columnOutput.flush();
        columnOutput.close();

        columnInput.close();
    }

    /**
     * 需要clearColumnInput进行map清理，否则可能oom
     *
     * @param pos
     * @param size
     * @return
     * @throws IOException
     */
    protected ByteBuffer read(long pos, int size) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        columnInput.seek(pos);
        int read = columnInput.read(byteBuffer.array());
        byteBuffer.limit(read);
        return byteBuffer;
    }
}