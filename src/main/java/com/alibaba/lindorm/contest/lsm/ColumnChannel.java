package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.structs.Aggregator;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public abstract class ColumnChannel<C extends ColumnValue> {

    protected final File columnFile;

    protected final DataChannel columnOutput;

    /**
     * todo lru
     */
//    private final RandomAccessFile columnInput;

    /**
     * 作用于shutdown时，没有满一批。
     */
    protected final File tmpIndexFile;

    protected int batchSize;

    protected long batchPos;

    protected int batchItemCount;

    protected boolean isDirty;

    public ColumnChannel(File vinDir, TableSchema.Column column) throws IOException {
        columnFile = new File(vinDir.getAbsolutePath(), column.columnName);
        if (!columnFile.exists()) {
            columnFile.createNewFile();
        }
        columnOutput = new DataChannel(columnFile, LsmStorage.IO_MODE, 8, LsmStorage.OUTPUT_BUFFER_SIZE);
//        columnInput = new RandomAccessFile(columnFile, "r");

        tmpIndexFile = new File(vinDir.getAbsolutePath(), column.columnName + ".shutdown");
        if (tmpIndexFile.exists()) {
            recoverTmpIndex();
            if (!tmpIndexFile.delete()) {
                System.out.println(("tmpIdxFile文件删除失败。"));
            }
        }

    }

    protected abstract void index(DataChannel columnIndexChannel) throws IOException;

    protected abstract void recoverTmpIndex() throws IOException;

    protected abstract void noNeedRecoverTmpIndex() throws IOException;

    protected abstract void shutdownTmpIndex() throws IOException;

    public void append(C c, DataChannel columnIndexChannel) throws IOException {
        isDirty = true;
        append0(c);
        batchItemCount++;
        batchSize += batchGrow(c);
        if (batchItemCount < LsmStorage.MAX_ITEM_CNT_L0) {
            return;
        }
        index(columnIndexChannel);
        batchPos = columnOutput.channelSize();
        batchItemCount = 0;
        batchSize = 0;
    }

    protected abstract List<? extends ColumnIndexItem> loadAllIndex() throws IOException;

    public abstract ColumnIndexItem readColumnIndexItem(ByteBuffer byteBuffer) throws IOException;

    protected abstract void append0(C c) throws IOException;

    protected abstract int batchGrow(C c) throws IOException;

    public abstract List<ColumnItem<C>> range(List<TimeItem> timeItemList, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException;

    public abstract ColumnValue agg(List<TimeItem> batchItemList, List<TimeItem> timeItemList, Aggregator aggregator,
                                    CompareExpression columnFilter, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException;

    public void shutdown() throws IOException {
        if (batchItemCount > 0) {
            if (!tmpIndexFile.exists()) {
                tmpIndexFile.createNewFile();
            }
            shutdownTmpIndex();
        }
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