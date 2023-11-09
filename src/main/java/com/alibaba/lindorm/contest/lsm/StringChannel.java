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

    public StringChannel(int vinHashCode, TableSchema.Column column, File columnFile, DataChannel columnOutput) throws IOException {
        super(vinHashCode, column, columnFile, columnOutput);
    }

    @Override
    protected void append0(List<ColumnValue.StringColumn> stringColumns) throws IOException {
        batchSize = 0;

        int size = 0;
        int[] sizeList = new int[stringColumns.size()];
        int i = 0;
        for (ColumnValue.StringColumn stringColumn : stringColumns) {
            sizeList[i++] = stringColumn.getStringValue().limit();
            size += stringColumn.getStringValue().limit();
        }
        ByteBuffer sizeListBuffer = NumberUtil.zIntDelta(sizeList);
        byte[] sizeListBytes = ByteBufferUtil.gZip(sizeListBuffer);
        if (sizeListBuffer.limit() < sizeListBytes.length) {
            columnOutput.writeByte((byte) 0);
            columnOutput.writeInt(sizeListBuffer.limit());
            batchSize += 1 + 4 + sizeListBuffer.limit();
            sizeListBuffer.flip();
            columnOutput.writeBytes(ByteBufferUtil.toBytes(sizeListBuffer));
        } else {
            columnOutput.writeByte((byte) 1);
            columnOutput.writeInt(sizeListBytes.length);
            batchSize += 1 + 4 + sizeListBytes.length;
            columnOutput.writeBytes(sizeListBytes);
        }

        ByteBuffer buffer = ByteBuffer.allocate(size);
        for (ColumnValue.StringColumn stringColumn : stringColumns) {
            buffer.put(stringColumn.getStringValue());
        }
        buffer.flip();
        // todo 长字符串序列用更高的压缩等级 todo hash位图编码
        int level = ((size - stringColumns.size()) / stringColumns.size()) < 10 ? 10 : 3;
        byte[] bytes = ByteBufferUtil.zstdEncode(buffer);
        batchSize += bytes.length;
        columnOutput.writeBytes(bytes);
        ORIG_SIZE.getAndAdd(size);
    }

    @Override
    protected void index(DataChannel columnIndexChannel, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException {
//        columnIndexChannel.writeLong(batchPos);
//        columnIndexChannel.writeInt(batchSize);
//        columnIndexChannel.writeInt(batchItemCount);

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
    public ColumnValue.StringColumn agg(List<TimeItem> batchItemList, List<TimeItem> timeItemList, Map<Long, List<Long>> batchTimeItemSetMap, Aggregator aggregator,
                                        CompareExpression columnFilter, Map<Long, ColumnIndexItem> columnIndexItemMap, List<ColumnValue.StringColumn> notcheckList) throws IOException {
        throw new IllegalStateException("string类型不支持聚合");
    }

    @Override
    public List<ColumnValue> aggDownSample(List<Map<Long, List<Long>>> batchTimeItemSetMapList, Aggregator aggregator, CompareExpression columnFilter, Map<Long, ColumnIndexItem> columnIndexItemMap, List<ColumnValue.StringColumn> notcheckList) throws IOException {
        throw new IllegalStateException("string类型不支持聚合");
    }

    @Override
    public List<ColumnItem<ColumnValue.StringColumn>> range(List<TimeItem> timeItemList, Map<Long, List<Long>> batchTimeItemSetMap, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException {
        flush();

        List<ColumnItem<ColumnValue.StringColumn>> columnItemList = new ArrayList<>(timeItemList.size());

        Collection<Long> batchNumList = batchTimeItemSetMap.keySet();
        Map<Long, Future<ByteBuffer>> futureMap = new HashMap<>();
        for (Long batchNum : batchNumList) {
            ColumnIndexItem columnIndexItem = columnIndexItemMap.get(batchNum);
            futureMap.put(batchNum, read(batchNum, columnIndexItem.getPos(), columnIndexItem.getSize()));
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

    private List<ColumnItem<ColumnValue.StringColumn>> range(long batchNum, long pos, int size, List<Long> batchItemSet, Future<ByteBuffer> bufferFuture) throws IOException {
        flush();

        List<ColumnItem<ColumnValue.StringColumn>> columnItemList = new ArrayList<>();

        ByteBuffer byteBuffer = null;
        try {
            byteBuffer = bufferFuture.get();
        } catch (Exception e) {
            throw new RuntimeException("获取buffer future failed.", e);
        }
        byte b = byteBuffer.get();
        int listSize = byteBuffer.getInt();
        ByteBuffer slice = byteBuffer.slice();
        slice.limit(listSize);
        if (b == 1) {
            slice = ByteBuffer.wrap(ByteBufferUtil.unGZip(slice));
        }
        List<Integer> sizeList = NumberUtil.rzIntDelta(slice);
        byteBuffer.position(1 + 4 + listSize);
        byteBuffer = ByteBuffer.wrap(ByteBufferUtil.zstdDecode(byteBuffer));
        long itemNum = batchNum * LsmStorage.MAX_ITEM_CNT_L0;
        int nextIdx = 0;
        int i = 0;
        do {
            ColumnValue.StringColumn column = readFrom(byteBuffer, sizeList.get(i++));
            if (batchItemSet.get(nextIdx).equals(itemNum)) {
                columnItemList.add(new ColumnItem<>(column, itemNum));
                nextIdx++;
            }
            itemNum++;
        } while (nextIdx < batchItemSet.size());
        return columnItemList;
    }

    private ColumnValue.StringColumn readFrom(ByteBuffer buffer, int strLen) {
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

