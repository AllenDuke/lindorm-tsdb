package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.structs.Aggregator;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class IntChannel extends ColumnChannel<ColumnValue.IntegerColumn> {

    private static final int FULL_BATCH_SIZE = (LsmStorage.MAX_ITEM_CNT_L0 - 1) * 4 + 4;

    private static final int TMP_IDX_SIZE = 4 + 4 + 8 + 4;

    private long batchSum;

    private int batchMax = Integer.MIN_VALUE;

    public IntChannel(File vinDir, TableSchema.Column column) throws IOException {
        super(vinDir, column);
    }

    @Override
    public void append0(ColumnValue.IntegerColumn integerColumn) throws IOException {
        int i = integerColumn.getIntegerValue();
        columnOutput.writeInt(i);
        batchSum += i;
        batchMax = Math.max(batchMax, i);
    }

    @Override
    protected void recoverTmpIndex() throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(TMP_IDX_SIZE);
        FileInputStream fileInputStream = new FileInputStream(tmpIndexFile);
        int read = fileInputStream.read(byteBuffer.array());
        if (read != TMP_IDX_SIZE) {
            throw new IllegalStateException("tmpIdxFile文件损坏。");
        }
        fileInputStream.close();

        batchItemCount = byteBuffer.getInt();
        batchSize = byteBuffer.getInt();
        batchSum = byteBuffer.getLong();
        batchMax = byteBuffer.getInt();
    }

    @Override
    protected void shutdownTmpIndex() throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(tmpIndexFile, false);
        ByteBuffer byteBuffer = ByteBuffer.allocate(TMP_IDX_SIZE);
        byteBuffer.putInt(batchItemCount);
        byteBuffer.putInt(batchSize);
        byteBuffer.putLong(batchSum);
        byteBuffer.putInt(batchMax);
        fileOutputStream.write(byteBuffer.array());
        fileOutputStream.flush();
        fileOutputStream.close();
    }

    @Override
    protected int batchGrow(ColumnValue.IntegerColumn integerColumn) throws IOException {
        return 4;
    }

    @Override
    protected void index() throws IOException {
        CommonUtils.writeLong(indexOutput, batchSum);
        CommonUtils.writeInt(indexOutput, batchMax);

        batchSum = 0;
        batchMax = Integer.MIN_VALUE;
    }

    @Override
    public List<ColumnItem<ColumnValue.IntegerColumn>> range(List<TimeItem> timeItemList) throws IOException {
        columnOutput.flush();

        List<ColumnItem<ColumnValue.IntegerColumn>> columnItemList = new ArrayList<>(timeItemList.size());

        Map<Long, Set<Long>> batchTimeItemSetMap = new HashMap<>();
        for (TimeItem timeItem : timeItemList) {
            Set<Long> timeItemSet = batchTimeItemSetMap.computeIfAbsent(timeItem.getBatchNum(), v -> new HashSet<>());
            timeItemSet.add(timeItem.getItemNum());
        }

        List<Long> batchNumList = batchTimeItemSetMap.keySet().stream().sorted().collect(Collectors.toList());
        for (Long batchNum : batchNumList) {
            ByteBuffer byteBuffer = read(batchNum * FULL_BATCH_SIZE, FULL_BATCH_SIZE);
            int pos = 0;
            int last = byteBuffer.getInt();
            long itemNum = batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos;
            if (batchTimeItemSetMap.get(batchNum).contains(itemNum)) {
                columnItemList.add(new ColumnItem<>(new ColumnValue.IntegerColumn(last), itemNum));
            }
            pos++;
            while (byteBuffer.remaining() > 0) {
                last = byteBuffer.getInt();
                itemNum = batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos;
                if (batchTimeItemSetMap.get(batchNum).contains(itemNum)) {
                    columnItemList.add(new ColumnItem<>(new ColumnValue.IntegerColumn(last), itemNum));
                }
                pos++;
            }
        }

        return columnItemList;
    }

    @Override
    public ColumnValue agg(List<TimeItem> timeItemList, Aggregator aggregator, CompareExpression columnFilter) throws IOException {
        columnOutput.flush();

        double sum = 0.0;
        int validCount = 0;
        int max = Integer.MIN_VALUE;

        Map<Long, Set<Long>> batchTimeItemSetMap = new HashMap<>();
        for (TimeItem timeItem : timeItemList) {
            Set<Long> timeItemSet = batchTimeItemSetMap.computeIfAbsent(timeItem.getBatchNum(), v -> new HashSet<>());
            timeItemSet.add(timeItem.getItemNum());
        }

        List<Long> batchNumList = batchTimeItemSetMap.keySet().stream().sorted().collect(Collectors.toList());
        for (Long batchNum : batchNumList) {
            ByteBuffer byteBuffer = read(batchNum * FULL_BATCH_SIZE, FULL_BATCH_SIZE);
            int pos = 0;
            int last = byteBuffer.getInt();
            long itemNum = batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos;
            if (batchTimeItemSetMap.get(batchNum).contains(itemNum) && (columnFilter == null || columnFilter.doCompare(new ColumnValue.IntegerColumn(last)))) {
                sum += last;
                validCount++;
                max = Math.max(max, last);
            }
            pos++;
            while (byteBuffer.remaining() > 0) {
                last = byteBuffer.getInt();
                itemNum = batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos;
                if (batchTimeItemSetMap.get(batchNum).contains(itemNum) && (columnFilter == null || columnFilter.doCompare(new ColumnValue.IntegerColumn(last)))) {
                    sum += last;
                    validCount++;
                    max = Math.max(max, last);
                }
                pos++;
            }
        }

        if (Aggregator.AVG.equals(aggregator)) {
            if (validCount == 0) {
                return new ColumnValue.DoubleFloatColumn(Double.NEGATIVE_INFINITY);
            }
            return new ColumnValue.DoubleFloatColumn(sum / validCount);
        }
        if (Aggregator.MAX.equals(aggregator)) {
            if (validCount == 0) {
                return new ColumnValue.IntegerColumn(Integer.MIN_VALUE);
            }
            return new ColumnValue.IntegerColumn(max);
        }
        throw new IllegalStateException("非法聚合函数");
    }
}
