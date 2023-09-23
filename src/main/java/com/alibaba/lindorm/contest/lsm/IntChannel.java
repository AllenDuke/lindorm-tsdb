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

    private static final int TMP_IDX_SIZE = 4 + 8 + 4 + 8 + 4 + 4;

    public static final int IDX_SIZE = 8 + 4 + 8 + 4;

    private long batchSum;

    private int batchMax;

    private int batchLastInt;

    private transient int appendSize;

    public IntChannel(File vinDir, TableSchema.Column column) throws IOException {
        super(vinDir, column);
    }

    @Override
    public void append0(ColumnValue.IntegerColumn integerColumn) throws IOException {
        int i = integerColumn.getIntegerValue();
        if (batchItemCount == 0) {
            columnOutput.writeInt(i);
            appendSize = 4;
        } else {
            appendSize = columnOutput.writeZInt(i - batchLastInt);
        }
        batchLastInt = i;
        batchSum += i;
        batchMax = Math.max(batchMax, i);
    }

    @Override
    protected void noNeedRecoverTmpIndex() throws IOException {
        batchMax = Integer.MIN_VALUE;
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
        batchPos = byteBuffer.getLong();
        batchSize = byteBuffer.getInt();
        batchSum = byteBuffer.getLong();
        batchMax = byteBuffer.getInt();
        batchLastInt = byteBuffer.getInt();
    }

    @Override
    protected void shutdownTmpIndex() throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(tmpIndexFile, false);
        ByteBuffer byteBuffer = ByteBuffer.allocate(TMP_IDX_SIZE);
        byteBuffer.putInt(batchItemCount);
        byteBuffer.putLong(batchPos);
        byteBuffer.putInt(batchSize);
        byteBuffer.putLong(batchSum);
        byteBuffer.putInt(batchMax);
        byteBuffer.putInt(batchLastInt);
        fileOutputStream.write(byteBuffer.array());
        fileOutputStream.flush();
        fileOutputStream.close();
    }

    @Override
    protected int batchGrow(ColumnValue.IntegerColumn integerColumn) throws IOException {
        return appendSize;
    }

    @Override
    public ColumnIndexItem readColumnIndexItem(ByteBuffer byteBuffer) throws IOException {
        long batchSum = byteBuffer.getLong();
        int batchMax = byteBuffer.getInt();
        long batchPos = byteBuffer.getLong();
        int batchSize = byteBuffer.getInt();
        return new IntIndexItem(-1, batchPos, batchSize, batchSum, batchMax);
    }

    @Override
    protected List<IntIndexItem> loadAllIndex() throws IOException {
//        FileInputStream fileInputStream = new FileInputStream(indexFile);
//        ByteBuffer byteBuffer = ByteBuffer.allocate((int) indexFile.length());
//        if (fileInputStream.read(byteBuffer.array()) != (int) indexFile.length()) {
//            throw new IllegalStateException("稀疏索引文件损坏");
//        }
//        fileInputStream.close();
//
//        if (byteBuffer.limit() % IDX_SIZE != 0) {
//            throw new IllegalStateException("稀疏索引文件损坏");
//        }
//        int indexItemCount = byteBuffer.limit() / IDX_SIZE;
//        List<IntIndexItem> indexItemList = new ArrayList<>(indexItemCount);
//
//        if (indexItemCount > 0) {
//            indexItemList.add(new IntIndexItem(0, 0, FULL_BATCH_SIZE, byteBuffer.getLong(), byteBuffer.getInt()));
//        }
//
//        for (int i = 1; i < indexItemCount; i++) {
//            IntIndexItem indexItem = indexItemList.get(i - 1);
//            indexItemList.add(new IntIndexItem(i, indexItem.getPos() + indexItem.getSize(), FULL_BATCH_SIZE, byteBuffer.getLong(), byteBuffer.getInt()));
//        }
//
//        if (batchItemCount > 0) {
//            if (indexItemCount > 0) {
//                IntIndexItem indexItem = indexItemList.get(indexItemCount - 1);
//                indexItemList.add(new IntIndexItem(indexItemCount, indexItem.getPos() + indexItem.getSize(), batchSize, batchSum, batchMax));
//            } else {
//                indexItemList.add(new IntIndexItem(indexItemCount, 0, batchSize, batchSum, batchMax));
//            }
//        }
//        return indexItemList;
        return null;
    }

    @Override
    protected void index(DataChannel columnIndexChannel) throws IOException {
        columnIndexChannel.writeLong(batchSum);
        columnIndexChannel.writeInt(batchMax);
        columnIndexChannel.writeLong(batchPos);
        columnIndexChannel.writeInt(batchSize);

        batchSum = 0;
        batchMax = Integer.MIN_VALUE;
    }

    @Override
    public List<ColumnItem<ColumnValue.IntegerColumn>> range(List<TimeItem> timeItemList, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException {
        columnOutput.flush();

        List<ColumnItem<ColumnValue.IntegerColumn>> columnItemList = new ArrayList<>(timeItemList.size());

        Map<Long, Set<Long>> batchTimeItemSetMap = new HashMap<>();
        for (TimeItem timeItem : timeItemList) {
            Set<Long> timeItemSet = batchTimeItemSetMap.computeIfAbsent(timeItem.getBatchNum(), v -> new HashSet<>());
            timeItemSet.add(timeItem.getItemNum());
        }

        List<Long> batchNumList = batchTimeItemSetMap.keySet().stream().sorted().collect(Collectors.toList());
        for (Long batchNum : batchNumList) {
            ColumnIndexItem columnIndexItem = columnIndexItemMap.get(batchNum);
            if (columnIndexItem == null) {
                // 半包批次
                columnIndexItem = new IntIndexItem(Math.toIntExact(batchNum), batchPos, batchSize, batchSum, batchMax);
            }
            ByteBuffer byteBuffer = read(columnIndexItem.getPos(), columnIndexItem.getSize());
            int pos = 0;
            int last = byteBuffer.getInt();
            long itemNum = batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos;
            if (batchTimeItemSetMap.get(batchNum).contains(itemNum)) {
                columnItemList.add(new ColumnItem<>(new ColumnValue.IntegerColumn(last), itemNum));
            }
            pos++;
            while (byteBuffer.remaining() > 0) {
                last += columnOutput.readZInt(byteBuffer);
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
    public ColumnValue agg(List<TimeItem> batchItemList, List<TimeItem> timeItemList, Aggregator aggregator,
                           CompareExpression columnFilter, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException {
        long sum = 0;
        int validCount = 0;
        int max = Integer.MIN_VALUE;

        Map<Long, Set<Long>> batchTimeItemSetMap = new HashMap<>();
        for (TimeItem timeItem : timeItemList) {
            if (timeItem.getTime() > 0) {
                Set<Long> timeItemSet = batchTimeItemSetMap.computeIfAbsent(timeItem.getBatchNum(), v -> new HashSet<>());
                timeItemSet.add(timeItem.getItemNum());
            }
        }
        if (!batchTimeItemSetMap.isEmpty()) {
            // 需要扫描数据列
            columnOutput.flush();
        }

        if (columnFilter == null && !batchItemList.isEmpty()) {
            for (TimeItem item : batchItemList) {
                long batchNum = item.getBatchNum();
                ColumnIndexItem columnIndexItem = columnIndexItemMap.get(batchNum);
                if (columnIndexItem == null) {
                    // 半包批次
                    columnIndexItem = new IntIndexItem(Math.toIntExact(batchNum), batchPos, batchSize, batchSum, batchMax);
                }
                ByteBuffer byteBuffer = read(columnIndexItem.getPos(), columnIndexItem.getSize());
                int last = byteBuffer.getInt();
                sum += last;
                validCount++;
                max = Math.max(max, last);
                while (byteBuffer.remaining() > 0) {
                    last += columnOutput.readZInt(byteBuffer);
                    sum += last;
                    validCount++;
                    max = Math.max(max, last);
                }
            }
        }

        List<Long> batchNumList = batchTimeItemSetMap.keySet().stream().sorted().collect(Collectors.toList());
        for (Long batchNum : batchNumList) {
            ColumnIndexItem columnIndexItem = columnIndexItemMap.get(batchNum);
            if (columnIndexItem == null) {
                // 半包批次
                columnIndexItem = new IntIndexItem(Math.toIntExact(batchNum), batchPos, batchSize, batchSum, batchMax);
            }
            ByteBuffer byteBuffer = read(columnIndexItem.getPos(), columnIndexItem.getSize());
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
                last += columnOutput.readZInt(byteBuffer);
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
}
