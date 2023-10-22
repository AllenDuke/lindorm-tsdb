package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.structs.Aggregator;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;
import com.alibaba.lindorm.contest.util.ByteBufferUtil;
import com.alibaba.lindorm.contest.util.NumberUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class IntChannel extends ColumnChannel<ColumnValue.IntegerColumn> {

    public static final AtomicLong ORIG_SIZE = new AtomicLong(0);
    public static final AtomicLong REAL_SIZE = new AtomicLong(0);

    public static final int IDX_SIZE = 8 + 4 + 8 + 4 + 4;

    private long batchSum;

    private int batchMax;

    public IntChannel(File vinDir, TableSchema.Column column, File columnFile, DataChannel columnOutput) throws IOException {
        super(vinDir, column, columnFile, columnOutput);
    }

    @Override
    protected void append0(List<ColumnValue.IntegerColumn> integerColumns) throws IOException {
        ORIG_SIZE.getAndAdd(4L * integerColumns.size());
        ByteBuffer buffer = this.zInt(integerColumns);
        byte[] bytes = ByteBufferUtil.gZip(buffer);
        batchSize = bytes.length;
        columnOutput.writeBytes(bytes);
    }

    @Override
    public ColumnIndexItem readColumnIndexItem(ByteBuffer byteBuffer) throws IOException {
        long batchSum = byteBuffer.getLong();
        int batchMax = byteBuffer.getInt();
        long batchPos = byteBuffer.getLong();
        int batchSize = byteBuffer.getInt();
        int batchItemCount = byteBuffer.getInt();
        return new IntIndexItem(-1, batchPos, batchSize, batchSum, batchMax, batchItemCount);
    }

    @Override
    protected void index(DataChannel columnIndexChannel, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException {
        columnIndexChannel.writeLong(batchSum);
        columnIndexChannel.writeInt(batchMax);
        columnIndexChannel.writeLong(batchPos);
        columnIndexChannel.writeInt(batchSize);
        columnIndexChannel.writeInt(batchItemCount);

        columnIndexItemMap.put((long) columnIndexItemMap.size(), new IntIndexItem(-1, batchPos, batchSize, batchSum, batchMax, batchItemCount));

        batchSum = 0;
        batchMax = Integer.MIN_VALUE;

        REAL_SIZE.getAndAdd(batchSize);
    }

    @Override
    public List<ColumnItem<ColumnValue.IntegerColumn>> range(List<TimeItem> timeItemList, Map<Long, Set<Long>> batchTimeItemSetMap, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException {
        columnOutput.flush();

        List<ColumnItem<ColumnValue.IntegerColumn>> columnItemList = new ArrayList<>(timeItemList.size());

        Collection<Long> batchNumList = batchTimeItemSetMap.keySet();
        for (Long batchNum : batchNumList) {
            ColumnIndexItem columnIndexItem = columnIndexItemMap.get(batchNum);
            ByteBuffer byteBuffer = read(columnIndexItem.getPos(), columnIndexItem.getSize());
            byteBuffer = ByteBuffer.wrap(ByteBufferUtil.unGZip(byteBuffer));

            List<Integer> ints = this.rzInt(byteBuffer);
            int pos = 0;
            long itemNum;
            for (Integer cur : ints) {
                itemNum = batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos++;
                if (batchTimeItemSetMap.get(batchNum).contains(itemNum)) {
                    columnItemList.add(new ColumnItem<>(new ColumnValue.IntegerColumn(cur), itemNum));
                }
            }
        }

        return columnItemList;
    }

    @Override
    public ColumnValue agg(List<TimeItem> batchItemList, List<TimeItem> timeItemList, Map<Long, Set<Long>> batchTimeItemSetMap, Aggregator aggregator,
                           CompareExpression columnFilter, Map<Long, ColumnIndexItem> columnIndexItemMap, List<ColumnValue.IntegerColumn> notcheckList) throws IOException {
        long sum = 0;
        int validCount = 0;
        int max = Integer.MIN_VALUE;

        if (columnFilter == null && !batchItemList.isEmpty()) {
            for (TimeItem item : batchItemList) {
                long batchNum = item.getBatchNum();
                IntIndexItem columnIndexItem = (IntIndexItem) columnIndexItemMap.get(batchNum);
                validCount += columnIndexItem.getBatchItemCount();
                sum += columnIndexItem.getBatchSum();
                max = Math.max(max, columnIndexItem.getBatchMax());

            }
//            AGG_HIT_IDX_CNT.getAndAdd(validCount);
        }

        Collection<Long> batchNumList = batchTimeItemSetMap.keySet();
        for (Long batchNum : batchNumList) {
            ColumnIndexItem columnIndexItem = columnIndexItemMap.get(batchNum);
            ByteBuffer byteBuffer = read(columnIndexItem.getPos(), columnIndexItem.getSize());

            byteBuffer = ByteBuffer.wrap(ByteBufferUtil.unGZip(byteBuffer));

            List<Integer> ints = this.rzInt(byteBuffer);
            int pos = 0;
            long itemNum;
            for (Integer cur : ints) {
                itemNum = batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos++;
                if (batchTimeItemSetMap.get(batchNum).contains(itemNum) && (columnFilter == null || columnFilter.doCompare(new ColumnValue.IntegerColumn(cur)))) {
                    sum += cur;
                    validCount++;
                    max = Math.max(max, cur);
                }
            }
        }

        for (ColumnValue.IntegerColumn columnValue : notcheckList) {
            if (columnFilter == null || columnFilter.doCompare(columnValue)) {
                sum += columnValue.getIntegerValue();
                validCount++;
                max = Math.max(max, columnValue.getIntegerValue());
            }
        }

        if (Aggregator.AVG.equals(aggregator)) {
            if (validCount == 0) {
                return new ColumnValue.DoubleFloatColumn(Double.NEGATIVE_INFINITY);
            }
            return new ColumnValue.DoubleFloatColumn((double) sum / validCount);
        }
        if (Aggregator.MAX.equals(aggregator)) {
            if (validCount == 0) {
                return new ColumnValue.IntegerColumn(Integer.MIN_VALUE);
            }
            return new ColumnValue.IntegerColumn(max);
        }
        throw new IllegalStateException("非法聚合函数");
    }

    @Override
    public void flush() throws IOException {
        if (!isDirty) {
            return;
        }
//        indexOutput.flush();
        super.flush();
        isDirty = false;
    }

    public List<Integer> rzInt(ByteBuffer buffer) throws IOException {
        List<Integer> ints = new ArrayList<>(buffer.limit() >> 2);
        int lastPre = buffer.getInt();
        int last = buffer.getInt();
        ints.add(lastPre);
        ints.add(last);
        while (buffer.hasRemaining()) {
            int cur = last - lastPre + last + zigZagDecode(readVInt(buffer));
            ints.add(cur);
            lastPre = last;
            last = cur;
        }
        return ints;
    }

    private int readVInt(ByteBuffer buffer) throws IOException {
        byte b = buffer.get();
        if (b >= 0) return b;
        int i = b & 0x7F;
        b = buffer.get();
        i |= (b & 0x7F) << 7;
        if (b >= 0) return i;
        b = buffer.get();
        i |= (b & 0x7F) << 14;
        if (b >= 0) return i;
        b = buffer.get();
        i |= (b & 0x7F) << 21;
        if (b >= 0) return i;
        b = buffer.get();
        // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
        i |= (b & 0x0F) << 28;
        if ((b & 0xF0) == 0) return i;
        throw new IOException("Invalid vInt detected (too many bits)");
    }

    public ByteBuffer zInt(List<ColumnValue.IntegerColumn> ints) {
        ByteBuffer buffer = ByteBuffer.allocate(ints.size() * 5);
        int lastPre = ints.get(0).getIntegerValue();
        batchSum += lastPre;
        batchMax = Math.max(batchMax, lastPre);
        buffer.putInt(lastPre);
        int last = ints.get(1).getIntegerValue();
        batchSum += last;
        batchMax = Math.max(batchMax, last);
        buffer.putInt(last);
        for (int i = 2; i < ints.size(); i++) {
            int v = ints.get(i).getIntegerValue();
            batchSum += v;
            batchMax = Math.max(batchMax, v);
            v = this.zigZagEncode(ints.get(i).getIntegerValue() - last - (last - lastPre));
            while ((v & ~0x7F) != 0) {
                buffer.put((byte) ((v & 0x7F) | 0x80));
                v >>>= 7;
            }
            buffer.put((byte) v);
            lastPre = last;
            last = ints.get(i).getIntegerValue();
        }
        buffer.flip();
        return buffer;
    }

    public int zigZagEncode(int i) {
        return (i >> 31) ^ (i << 1);
    }

    public int zigZagDecode(int i) {
        return ((i >>> 1) ^ -(i & 1));
    }
}
