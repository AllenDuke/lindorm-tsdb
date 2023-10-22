package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.elf.ElfOnChimpCompressor;
import com.alibaba.lindorm.contest.elf.ElfOnChimpDecompressor;
import com.alibaba.lindorm.contest.elf.ICompressor;
import com.alibaba.lindorm.contest.elf.IDecompressor;
import com.alibaba.lindorm.contest.structs.Aggregator;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;
import com.alibaba.lindorm.contest.util.ByteBufferUtil;
import com.alibaba.lindorm.contest.util.NumberUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.alibaba.lindorm.contest.CommonUtils.ARRAY_BASE_OFFSET;
import static com.alibaba.lindorm.contest.CommonUtils.UNSAFE;

public class DoubleChannel extends ColumnChannel<ColumnValue.DoubleFloatColumn> {

    public static final AtomicLong ORIG_SIZE = new AtomicLong(0);
    public static final AtomicLong REAL_SIZE = new AtomicLong(0);

    public static final int IDX_SIZE = 8 + 8 + 8 + 4 + 4 + 4;

    private double batchSum;

    private double batchMax;

    private int batchMaxScale = 12;

    public DoubleChannel(File vinDir, TableSchema.Column column, File columnFile, DataChannel columnOutput) throws IOException {
        super(vinDir, column, columnFile, columnOutput);
    }

    @Override
    public void append0(List<ColumnValue.DoubleFloatColumn> doubleFloatColumns) throws IOException {
        ORIG_SIZE.getAndAdd(8 * doubleFloatColumns.size());
        byte[] encode = elf(doubleFloatColumns);
        batchSize = encode.length;
        columnOutput.writeBytes(encode);
    }

    @Override
    public ColumnIndexItem readColumnIndexItem(ByteBuffer byteBuffer) throws IOException {
        double batchSum = byteBuffer.getDouble();
        double batchMax = byteBuffer.getDouble();
        long batchPos = byteBuffer.getLong();
        int batchSize = byteBuffer.getInt();
        int batchMaxScale = byteBuffer.getInt();
        int batchItemCount = byteBuffer.getInt();
        DoubleIndexItem doubleIndexItem = new DoubleIndexItem(-1, batchPos, batchSize, batchSum, batchMax, batchMaxScale, batchItemCount);
        return doubleIndexItem;
    }

    @Override
    protected void index(DataChannel columnIndexChannel, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException {
        columnIndexChannel.writeDouble(batchSum);
        columnIndexChannel.writeDouble(batchMax);
        columnIndexChannel.writeLong(batchPos);
        columnIndexChannel.writeInt(batchSize);
        columnIndexChannel.writeInt(batchMaxScale);
        columnIndexChannel.writeInt(batchItemCount);

        columnIndexItemMap.put((long) columnIndexItemMap.size(), new DoubleIndexItem(-1, batchPos, batchSize, batchSum, batchMax, batchMaxScale, batchItemCount));

        batchSum = 0;
        batchMax = -Double.MAX_VALUE;
        batchMaxScale = 12;

        REAL_SIZE.getAndAdd(batchSize);
    }

    @Override
    public List<ColumnItem<ColumnValue.DoubleFloatColumn>> range(List<TimeItem> timeItemList, Map<Long, Set<Long>> batchTimeItemSetMap, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException {
        super.flush();

        List<ColumnItem<ColumnValue.DoubleFloatColumn>> columnItemList = new ArrayList<>(timeItemList.size());

        Collection<Long> batchNumList = batchTimeItemSetMap.keySet();
        Map<Long, Future<ByteBuffer>> futureMap = new HashMap<>();
        for (Long batchNum : batchNumList) {
            ColumnIndexItem columnIndexItem = columnIndexItemMap.get(batchNum);
            futureMap.put(batchNum, read(columnIndexItem.getPos(), columnIndexItem.getSize()));
        }
        for (Long batchNum : batchNumList) {
            ByteBuffer byteBuffer = null;
            try {
                byteBuffer = futureMap.get(batchNum).get();
            } catch (Exception e) {
                throw new RuntimeException("获取buffer future failed.", e);
            }
            List<Double> doubles = unElf(byteBuffer);
            int pos = 0;
            long itemNum;
            for (Double last : doubles) {
                itemNum = batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos;
                if (batchTimeItemSetMap.get(batchNum).contains(itemNum)) {
                    columnItemList.add(new ColumnItem<>(new ColumnValue.DoubleFloatColumn(last), itemNum));
                }
                pos++;
            }
        }
        return columnItemList;
    }

    @Override
    public ColumnValue.DoubleFloatColumn agg(List<TimeItem> batchItemList, List<TimeItem> timeItemList, Map<Long, Set<Long>> batchTimeItemSetMap, Aggregator aggregator,
                                             CompareExpression columnFilter, Map<Long, ColumnIndexItem> columnIndexItemMap, List<ColumnValue.DoubleFloatColumn> notcheckList) throws IOException {
        double sum = 0.0;
        double max = -Double.MAX_VALUE;
        int validCount = 0;

        if (columnFilter == null && !batchItemList.isEmpty()) {
            for (TimeItem item : batchItemList) {
                long batchNum = item.getBatchNum();
                DoubleIndexItem columnIndexItem = (DoubleIndexItem) columnIndexItemMap.get(batchNum);
                validCount += columnIndexItem.getBatchItemCount();
                sum += columnIndexItem.getBatchSum();
                max = Math.max(max, columnIndexItem.getBatchMax());
            }
        }

        Collection<Long> batchNumList = batchTimeItemSetMap.keySet();
        Map<Long, Future<ByteBuffer>> futureMap = new HashMap<>();
        for (Long batchNum : batchNumList) {
            ColumnIndexItem columnIndexItem = columnIndexItemMap.get(batchNum);
            futureMap.put(batchNum, read(columnIndexItem.getPos(), columnIndexItem.getSize()));
        }
        for (Long batchNum : batchNumList) {
            ByteBuffer byteBuffer = null;
            try {
                byteBuffer = futureMap.get(batchNum).get();
            } catch (Exception e) {
                throw new RuntimeException("获取buffer future failed.", e);
            }
            List<Double> doubles = unElf(byteBuffer);
            int pos = 0;
            long itemNum;
            for (Double last : doubles) {
                itemNum = batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos;
                if (batchTimeItemSetMap.get(batchNum).contains(itemNum) && (columnFilter == null || columnFilter.doCompare(new ColumnValue.DoubleFloatColumn(last)))) {
                    sum += last;
                    validCount++;
                    max = Math.max(last, max);

                }
                pos++;
            }
        }

        for (ColumnValue.DoubleFloatColumn columnValue : notcheckList) {
            if (columnFilter == null || columnFilter.doCompare(columnValue)) {
                sum += columnValue.getDoubleFloatValue();
                validCount++;
                max = Math.max(max, columnValue.getDoubleFloatValue());
            }
        }

        if (validCount == 0) {
            return new ColumnValue.DoubleFloatColumn(Double.NEGATIVE_INFINITY);
        }
        if (Aggregator.AVG.equals(aggregator)) {
            return new ColumnValue.DoubleFloatColumn(sum / validCount);
        }
        if (Aggregator.MAX.equals(aggregator)) {
            return new ColumnValue.DoubleFloatColumn(max);
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

    public byte[] elf(List<ColumnValue.DoubleFloatColumn> doubleFloatColumns) throws IOException {
        ICompressor compressor = new ElfOnChimpCompressor(DataChannel.BUFFER_SIZE * 4);
        for (ColumnValue.DoubleFloatColumn doubleFloatColumn : doubleFloatColumns) {
            double v = doubleFloatColumn.getDoubleFloatValue();
            compressor.addValue(v);
            batchSum += v;
            batchMax = Math.max(batchMax, v);
        }
        compressor.close();
        byte[] encode = new byte[compressor.getSize()];
//        UNSAFE.copyMemory(compressor.getBytes(), 0, encode, 0, compressor.getSize());
        System.arraycopy(compressor.getBytes(), 0, encode, 0, compressor.getSize());
        return encode;
    }

    public List<Double> unElf(ByteBuffer buffer) throws IOException {
        List<Double> doubles = new ArrayList<>();
        byte[] array1 = ByteBufferUtil.toBytes(buffer);
        IDecompressor decompressor = new ElfOnChimpDecompressor(array1);
        List<Double> values = decompressor.decompress();
        for (int i = 0; i < values.size(); i++) {
            Double v = values.get(i);
            doubles.add(v);
        }
        return doubles;
    }
}
