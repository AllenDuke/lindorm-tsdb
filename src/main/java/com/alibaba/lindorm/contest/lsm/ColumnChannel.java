package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.structs.Aggregator;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ColumnChannel<C extends ColumnValue> {

    public static AtomicLong AGG_HIT_IDX_CNT = new AtomicLong(0);
    public static AtomicLong AGG_CNT = new AtomicLong(0);

    public static AtomicLong AGG_LOG_CNT = new AtomicLong(0);

    protected final File columnFile;

    protected final DataChannel columnOutput;

    /**
     * todo lru
     */
//    private final RandomAccessFile columnInput;

    protected int batchSize;

    protected long batchPos;

    protected int batchItemCount;

    protected boolean isDirty;

    public ColumnChannel(File vinDir, TableSchema.Column column, File columnFile, DataChannel columnOutput) throws IOException {
        this.columnFile = columnFile;
        this.columnOutput = columnOutput;
//        columnInput = new RandomAccessFile(columnFile, "r");
    }

    protected abstract void index(DataChannel columnIndexChannel, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException;

    public void append(List<C> cList, DataChannel columnIndexChannel, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException {
        isDirty = true;
        batchPos = columnOutput.channelSize();
        append0(cList);
        index(columnIndexChannel, columnIndexItemMap);
    }

    public abstract ColumnIndexItem readColumnIndexItem(ByteBuffer byteBuffer) throws IOException;

    protected abstract void append0(List<C> cList) throws IOException;

    public abstract List<ColumnItem<C>> range(List<TimeItem> timeItemList, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException;

    public abstract ColumnValue agg(List<TimeItem> batchItemList, List<TimeItem> timeItemList, Aggregator aggregator,
                                    CompareExpression columnFilter, Map<Long, ColumnIndexItem> columnIndexItemMap, List<C> notcheckList) throws IOException;

    public void shutdown() throws IOException {
        flush();
        columnOutput.close();

//        columnInput.close();
    }

    public void flush() throws IOException {
        if (!isDirty) {
            return;
        }
        columnOutput.flush();
        isDirty = false;
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
        return columnOutput.read(pos, size);
    }
}