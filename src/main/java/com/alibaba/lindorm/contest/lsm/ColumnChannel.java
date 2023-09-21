package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.structs.Aggregator;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;

public abstract class ColumnChannel<C extends ColumnValue> {

    protected final File columnFile;

    protected final DataChannel columnOutput;

    /**
     * todo lru
     */
    private final RandomAccessFile columnInput;

    protected final OutputStream indexOutput;

    protected final File indexFile;

    /**
     * 作用于shutdown时，没有满一批。
     */
    protected final File tmpIndexFile;

    protected int batchSize;

    protected int batchItemCount;

    public ColumnChannel(File vinDir, TableSchema.Column column) throws IOException {
        columnFile = new File(vinDir.getAbsolutePath(), column.columnName);
        if (!columnFile.exists()) {
            columnFile.createNewFile();
        }
        columnOutput = new DataChannel(columnFile, LsmStorage.IO_MODE, 8, LsmStorage.OUTPUT_BUFFER_SIZE);
        columnInput = new RandomAccessFile(columnFile, "r");

        indexFile = new File(vinDir.getAbsolutePath(), column.columnName + ".idx");
        if (!indexFile.exists()) {
            indexFile.createNewFile();
        }
        indexOutput = new BufferedOutputStream(new FileOutputStream(indexFile, true), LsmStorage.OUTPUT_BUFFER_SIZE);

        tmpIndexFile = new File(vinDir.getAbsolutePath(), column.columnName + ".tmp");
        if (tmpIndexFile.exists()) {
            recoverTmpIndex();
            if (!tmpIndexFile.delete()) {
                System.out.println(("tmpIdxFile文件删除失败。"));
            }
        }
    }

    protected abstract void index() throws IOException;

    protected abstract void recoverTmpIndex() throws IOException;

    protected abstract void shutdownTmpIndex() throws IOException;

    public void append(C c) throws IOException {
        append0(c);
        batchItemCount++;
        batchSize += batchGrow(c);
        if (batchItemCount < LsmStorage.MAX_ITEM_CNT_L0) {
            return;
        }
        index();
        batchItemCount = 0;
        batchSize = 0;
    }

    protected abstract List<? extends ColumnIndexItem> loadAllIndex() throws IOException;

    protected abstract void append0(C c) throws IOException;

    protected abstract int batchGrow(C c) throws IOException;

    public abstract List<ColumnItem<C>> range(List<TimeItem> timeItemList) throws IOException;

    public abstract ColumnValue agg(List<TimeItem> batchItemList, List<TimeItem> timeItemList, Aggregator aggregator, CompareExpression columnFilter) throws IOException;

    public void shutdown() throws IOException {
        if (batchItemCount > 0) {
            if (!tmpIndexFile.exists()) {
                tmpIndexFile.createNewFile();
            }
            shutdownTmpIndex();
        }
        flush();
        columnOutput.close();

        columnInput.close();
    }

    public void flush() throws IOException {
        columnOutput.flush();
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