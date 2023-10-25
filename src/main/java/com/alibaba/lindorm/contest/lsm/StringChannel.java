package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.structs.Aggregator;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;
import com.alibaba.lindorm.contest.util.ByteBufferUtil;
import com.alibaba.lindorm.contest.util.NumberUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class StringChannel extends ColumnChannel<ColumnValue.StringColumn> {

    public static final int IDX_SIZE = 8 + 4 + 4;

    public static final AtomicLong ORIG_SIZE = new AtomicLong(0);
    public static final AtomicLong REAL_SIZE = new AtomicLong(0);

    public StringChannel(File vinDir, TableSchema.Column column, File columnFile, DataChannel columnOutput) throws IOException {
        super(vinDir, column, columnFile, columnOutput);
    }

    private int encode = -1;

    @Override
    protected void append0(List<ColumnValue.StringColumn> stringColumns) throws IOException {
        int size = 0;
        for (ColumnValue.StringColumn stringColumn : stringColumns) {
            size += 4 + stringColumn.getStringValue().limit();
        }
        byte[] bytes = null;
        if (encode == 0) {
            byte[] zstd = zstd(stringColumns, size);
            bytes = zstd;
        }
        if (encode == 1) {
            byte[] hash = hash(stringColumns);
            bytes = hash;
        }
        if (encode == -1) {
            byte[] zstd = zstd(stringColumns, size);
            byte[] hash = hash(stringColumns);
            if (zstd.length <= hash.length) {
                bytes = zstd;
                encode = 0;
            } else {
                bytes = hash;
                encode = 1;
            }
        }
        columnOutput.writeByte((byte) encode);
        batchSize = bytes.length + 1;
        columnOutput.writeBytes(bytes);
        ORIG_SIZE.getAndAdd(size);
    }

    private byte[] zstd(List<ColumnValue.StringColumn> stringColumns, int size) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        for (ColumnValue.StringColumn stringColumn : stringColumns) {
            buffer.putInt(stringColumn.getStringValue().limit());
            buffer.put(stringColumn.getStringValue());
        }
        byte[] bytes = ByteBufferUtil.zstdEncode(buffer.array());
        return bytes;
    }

    private byte[] hash(List<ColumnValue.StringColumn> stringColumns) throws IOException {
        int size = 0;
        // 488288
        Map<ByteBuffer, Integer> map = new LinkedHashMap<>(stringColumns.size() >> 2);
        List<Integer> ints = new ArrayList<>(stringColumns.size());
        for (ColumnValue.StringColumn stringColumn : stringColumns) {
            Integer integer = map.get(stringColumn.getStringValue());
            if (integer != null) {
                ints.add(integer);
            } else {
                ints.add(map.size());
                map.put(stringColumn.getStringValue(), map.size());
                size += 4 + stringColumn.getStringValue().limit();
            }
        }
        ByteBuffer zInt = NumberUtil.zInt(ints);
        ByteBuffer buffer = ByteBuffer.allocate(4 + zInt.limit() + size);
        buffer.putInt(zInt.limit());
        buffer.put(zInt);
        for (ByteBuffer byteBuffer : map.keySet()) {
            buffer.putInt(byteBuffer.limit());
            buffer.put(byteBuffer);
        }
        byte[] bytes = ByteBufferUtil.zstdEncode(buffer.array());
        return bytes;
    }

    @Override
    protected void index(DataChannel columnIndexChannel, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException {
        columnIndexChannel.writeLong(batchPos);
        columnIndexChannel.writeInt(batchSize);
        columnIndexChannel.writeInt(batchItemCount);

        columnIndexItemMap.put((long) columnIndexItemMap.size(), new StringIndexItem(-1, batchPos, batchSize, batchItemCount));

        REAL_SIZE.getAndAdd(batchSize);
    }

    @Override
    public ColumnIndexItem readColumnIndexItem(ByteBuffer byteBuffer) throws IOException {
        long batchPos = byteBuffer.getLong();
        int batchSize = byteBuffer.getInt();
        int batchItemCount = byteBuffer.getInt();
        StringIndexItem stringIndexItem = new StringIndexItem(-1, batchPos, batchSize, batchItemCount);
        return stringIndexItem;
    }

    @Override
    public ColumnValue.StringColumn agg(List<TimeItem> batchItemList, List<TimeItem> timeItemList, Map<Long, Set<Long>> batchTimeItemSetMap, Aggregator aggregator,
                                        CompareExpression columnFilter, Map<Long, ColumnIndexItem> columnIndexItemMap, List<ColumnValue.StringColumn> notcheckList) throws IOException {
        throw new IllegalStateException("string类型不支持聚合");
    }

    @Override
    public List<ColumnItem<ColumnValue.StringColumn>> range(List<TimeItem> timeItemList, Map<Long, Set<Long>> batchTimeItemSetMap, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException {
        flush();

        List<ColumnItem<ColumnValue.StringColumn>> columnItemList = new ArrayList<>(timeItemList.size());

        Collection<Long> batchNumList = batchTimeItemSetMap.keySet();
        Map<Long, Future<ByteBuffer>> futureMap = new HashMap<>();
        for (Long batchNum : batchNumList) {
            ColumnIndexItem columnIndexItem = columnIndexItemMap.get(batchNum);
            futureMap.put(batchNum, read(columnIndexItem.getPos(), columnIndexItem.getSize()));
        }
        for (Long batchNum : batchNumList) {
            ColumnIndexItem columnIndexItem = columnIndexItemMap.get(batchNum);
            columnItemList.addAll(range(batchNum, columnIndexItem.getPos(), columnIndexItem.getSize(), batchTimeItemSetMap.get(batchNum), futureMap.get(batchNum)));
        }
        return columnItemList;
    }

    @Override
    public void flush() throws IOException {
//        indexOutput.flush();
        super.flush();
    }

    private List<ColumnItem<ColumnValue.StringColumn>> range(long batchNum, long pos, int size, Set<Long> batchItemSet, Future<ByteBuffer> bufferFuture) throws IOException {
        flush();

        List<ColumnItem<ColumnValue.StringColumn>> columnItemList = new ArrayList<>();

        ByteBuffer byteBuffer = null;
        try {
            byteBuffer = bufferFuture.get();
        } catch (Exception e) {
            throw new RuntimeException("获取buffer future failed.", e);
        }
        byte b = byteBuffer.get();
        byteBuffer = ByteBuffer.wrap(ByteBufferUtil.zstdDecode(byteBuffer));
        if (b == 0) {
            long itemNum = batchNum * LsmStorage.MAX_ITEM_CNT_L0;
            do {
                ColumnValue.StringColumn column = readFrom(byteBuffer);
                if (batchItemSet.contains(itemNum)) {
                    columnItemList.add(new ColumnItem<>(column, itemNum));
                }
                itemNum++;
            } while (byteBuffer.hasRemaining());
            return columnItemList;
        }
        int intSize = byteBuffer.getInt();
        ByteBuffer slice = byteBuffer.slice();
        slice.limit(intSize);
        List<Integer> ints = NumberUtil.rzInt(slice);
        byteBuffer.position(4 + intSize);
        Map<Integer, ColumnValue.StringColumn> map = new HashMap<>(ints.size() >> 2);
        long itemNum = LsmStorage.MAX_ITEM_CNT_L0 * batchNum;
        do {
            ColumnValue.StringColumn column = readFrom(byteBuffer);
            if (batchItemSet.contains(itemNum)) {
                columnItemList.add(new ColumnItem<>(column, itemNum));
            }
            map.put((int) itemNum, column);
            itemNum++;
        } while (byteBuffer.hasRemaining());

        itemNum = LsmStorage.MAX_ITEM_CNT_L0 * batchNum;
        for (int i = 0; i < ints.size(); i++) {
            if (batchItemSet.contains(itemNum)) {
                columnItemList.add(new ColumnItem<>(map.get(ints.get(i)), itemNum));
            }
            itemNum++;
        }

        return columnItemList;
    }

    private ColumnValue.StringColumn readFrom(ByteBuffer buffer) {
        int strLen = buffer.getInt();
        boolean zip = false;
        if (strLen < 0) {
            zip = true;
            strLen = -strLen;
        }

        byte[] strBytes = new byte[strLen];
        buffer.get(strBytes);
        ByteBuffer wrap;
        if (zip) {
            wrap = ByteBuffer.wrap(CommonUtils.unGZip(strBytes));
        } else {
            wrap = ByteBuffer.wrap(strBytes);
        }

        return new ColumnValue.StringColumn(wrap);
    }

    @Override
    public void shutdown() throws IOException {
//        indexOutput.flush();
//        indexOutput.close();
        super.shutdown();
    }
}
