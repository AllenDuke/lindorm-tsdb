package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.structs.Aggregator;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class DoubleChannel extends ColumnChannel<ColumnValue.DoubleFloatColumn> {

    private static final int FULL_BATCH_SIZE = (LsmStorage.MAX_ITEM_CNT_L0 - 1) * 8 + 8;

    private static final int TMP_IDX_SIZE = 4 + 4 + 8 + 8;

    private double batchSum;

    private double batchMax = -Double.MAX_VALUE;

    public DoubleChannel(File vinDir, TableSchema.Column column) throws IOException {
        super(vinDir, column);
    }

    @Override
    public void append0(ColumnValue.DoubleFloatColumn doubleFloatColumn) throws IOException {
        double v = doubleFloatColumn.getDoubleFloatValue();
        columnOutput.writeDouble(v);
        batchSum += v;
        batchMax = Math.max(batchMax, v);
    }

    @Override
    protected void index() throws IOException {
        CommonUtils.writeDouble(indexOutput, batchSum);
        CommonUtils.writeDouble(indexOutput, batchMax);

        batchSum = 0;
        batchMax = -Double.MAX_VALUE;
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
        batchSum = byteBuffer.getDouble();
        batchMax = byteBuffer.getDouble();
    }

    @Override
    protected void shutdownTmpIndex() throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(tmpIndexFile, false);
        ByteBuffer byteBuffer = ByteBuffer.allocate(TMP_IDX_SIZE);
        byteBuffer.putInt(batchItemCount);
        byteBuffer.putInt(batchSize);
        byteBuffer.putDouble(batchSum);
        byteBuffer.putDouble(batchMax);
        fileOutputStream.write(byteBuffer.array());
        fileOutputStream.flush();
        fileOutputStream.close();
    }

    @Override
    protected int batchGrow(ColumnValue.DoubleFloatColumn doubleFloatColumn) throws IOException {
        return 0;
    }

    @Override
    public List<ColumnItem<ColumnValue.DoubleFloatColumn>> range(List<TimeItem> timeItemList) throws IOException {
        super.flush();

        List<ColumnItem<ColumnValue.DoubleFloatColumn>> columnItemList = new ArrayList<>(timeItemList.size());

        Map<Long, Set<Long>> batchTimeItemSetMap = new HashMap<>();
        for (TimeItem timeItem : timeItemList) {
            Set<Long> timeItemSet = batchTimeItemSetMap.computeIfAbsent(timeItem.getBatchNum(), v -> new HashSet<>());
            timeItemSet.add(timeItem.getItemNum());
        }

        List<Long> batchNumList = batchTimeItemSetMap.keySet().stream().sorted().collect(Collectors.toList());
        for (Long batchNum : batchNumList) {
            ByteBuffer byteBuffer = read(batchNum * FULL_BATCH_SIZE, FULL_BATCH_SIZE);
            int pos = 0;
            double last = byteBuffer.getDouble();
            long itemNum = batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos;
            if (batchTimeItemSetMap.get(batchNum).contains(itemNum)) {
                columnItemList.add(new ColumnItem<>(new ColumnValue.DoubleFloatColumn(last), itemNum));
            }
            pos++;
            while (byteBuffer.remaining() > 0) {
                last = byteBuffer.getDouble();
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
    public ColumnValue.DoubleFloatColumn agg(List<TimeItem> timeItemList, Aggregator aggregator, CompareExpression columnFilter) throws IOException {
        super.flush();

        double sum = 0.0;
        double max = -Double.MAX_VALUE;
        int validCount = 0;

        Map<Long, Set<Long>> batchTimeItemSetMap = new HashMap<>();
        for (TimeItem timeItem : timeItemList) {
            Set<Long> timeItemSet = batchTimeItemSetMap.computeIfAbsent(timeItem.getBatchNum(), v -> new HashSet<>());
            timeItemSet.add(timeItem.getItemNum());
        }

        List<Long> batchNumList = batchTimeItemSetMap.keySet().stream().sorted().collect(Collectors.toList());
        for (Long batchNum : batchNumList) {
            ByteBuffer byteBuffer = read(batchNum * FULL_BATCH_SIZE, FULL_BATCH_SIZE);
            int pos = 0;
            double last = byteBuffer.getDouble();
            long itemNum = batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos;
            if (batchTimeItemSetMap.get(batchNum).contains(itemNum) && (columnFilter == null || columnFilter.doCompare(new ColumnValue.DoubleFloatColumn(last)))) {
                sum += last;
                validCount++;
                max = Math.max(last, max);
            }
            pos++;
            while (byteBuffer.remaining() > 0) {
                last = byteBuffer.getDouble();
                itemNum = batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos;
                if (batchTimeItemSetMap.get(batchNum).contains(itemNum) && (columnFilter == null || columnFilter.doCompare(new ColumnValue.DoubleFloatColumn(last)))) {
                    sum += last;
                    validCount++;
                    max = Math.max(last, max);

                }
                pos++;
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
}
