package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.structs.Aggregator;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.lindorm.contest.TSDBEngineImpl.IO_EXECUTOR;

public abstract class ColumnChannel<C extends ColumnValue> {

    public static AtomicLong AGG_HIT_IDX_CNT = new AtomicLong(0);
    public static AtomicLong AGG_CNT = new AtomicLong(0);

    public static AtomicLong AGG_LOG_CNT = new AtomicLong(0);

    /**
     * 所有vin共享一个大缓存池。
     * BYTE_BUFFER_MAP，k高32位为vin.hashcode
     */
    private static final AtomicLong AVAILABLE = new AtomicLong(4L * 1024 * 1024 * 1024 + 512);
    private static final Map<Long, ByteBuffer> BYTE_BUFFER_MAP = Collections.synchronizedMap(new LinkedHashMap<>());

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

    private final int vinHashCode;

    public ColumnChannel(int vinHashCode, TableSchema.Column column, File columnFile, DataChannel columnOutput) throws IOException {
        this.vinHashCode = vinHashCode;
        this.columnFile = columnFile;
        this.columnOutput = columnOutput;
//        columnInput = new RandomAccessFile(columnFile, "r");
    }

    protected abstract void index(DataChannel columnIndexChannel, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException;

    public void append(List<C> cList, DataChannel columnIndexChannel, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException {
        isDirty = true;
        batchPos = columnOutput.channelSize();
        batchItemCount = cList.size();
        append0(cList);
        index(columnIndexChannel, columnIndexItemMap);
    }

    public abstract ColumnIndexItem readColumnIndexItem(ByteBuffer byteBuffer) throws IOException;

    protected abstract void append0(List<C> cList) throws IOException;

    public abstract List<ColumnItem<C>> range(List<TimeItem> timeItemList, Map<Long, Set<Long>> batchTimeItemSetMap, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException;

    public abstract ColumnValue agg(List<TimeItem> batchItemList, List<TimeItem> timeItemList, Map<Long, Set<Long>> batchTimeItemSetMap, Aggregator aggregator,
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

    private long byteBufferMapK(long pos) {
        // todo 换成vinId
        long k = vinHashCode;
        k = k << 32;
        k = k | pos;
        return k;
    }

    /**
     * 需要clearColumnInput进行map清理，否则可能oom
     *
     * @param pos
     * @param size
     * @return
     * @throws IOException
     */
    protected Future<ByteBuffer> read(long batchNum, long pos, int size) throws IOException {
//        return IO_EXECUTOR.submit(() -> columnOutput.read(pos, size));
        return IO_EXECUTOR.submit(() -> {
            long k = byteBufferMapK(pos);
            ByteBuffer byteBuffer = BYTE_BUFFER_MAP.get(k);
            if (byteBuffer != null) {
                byteBuffer.clear();
            } else {
                byteBuffer = columnOutput.read(pos, size);
                BYTE_BUFFER_MAP.put(k, byteBuffer);
                AVAILABLE.addAndGet(-byteBuffer.capacity());
                Iterator<Map.Entry<Long, ByteBuffer>> entryIterator = BYTE_BUFFER_MAP.entrySet().iterator();
                while (entryIterator.hasNext()) {
                    if (AVAILABLE.get() > 0) {
                        break;
                    }
                    ByteBuffer remove = entryIterator.next().getValue();
                    AVAILABLE.addAndGet(remove.capacity());
                    entryIterator.remove();
                }
            }
            return byteBuffer;
        });
    }
}