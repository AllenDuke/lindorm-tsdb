package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.structs.Aggregator;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;

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

    private static final int FULL_BATCH_SIZE = (LsmStorage.MAX_ITEM_CNT_L0 - 1) * 4 + 4;

    private static final int TMP_IDX_SIZE = 4 + 8 + 4 + 8 + 4 + 4 + 4;

    public static final int IDX_SIZE = 8 + 4 + 8 + 4;

    private long batchSum;

    private int batchMax;

    private int batchLastInt;

    private int batchLastIntPre;

    private transient int appendSize;

    public IntChannel(File vinDir, TableSchema.Column column) throws IOException {
        super(vinDir, column);
    }

    @Override
    public void append0(ColumnValue.IntegerColumn integerColumn) throws IOException {
        ORIG_SIZE.getAndAdd(4);
        int i = integerColumn.getIntegerValue();
        if (batchItemCount == 0 || batchItemCount == 1) {
            columnOutput.writeInt(i);
            appendSize = 4;
        } else {
            appendSize = columnOutput.writeZInt(i - batchLastInt - (batchLastInt - batchLastIntPre));
        }
        batchLastIntPre = batchLastInt;
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
        batchLastIntPre = byteBuffer.getInt();
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
        byteBuffer.putInt(batchLastIntPre);
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
    protected void index(DataChannel columnIndexChannel, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException {
        columnOutput.flush();
        int batchGzipSize = columnOutput.batchGzip(batchPos, batchSize);

        columnIndexChannel.writeLong(batchSum);
        columnIndexChannel.writeInt(batchMax);
        columnIndexChannel.writeLong(batchPos);
        columnIndexChannel.writeInt(batchGzipSize);

        columnIndexItemMap.put((long) columnIndexItemMap.size(), new IntIndexItem(-1, batchPos, batchGzipSize, batchSum, batchMax));

        batchSum = 0;
        batchMax = Integer.MIN_VALUE;

        REAL_SIZE.getAndAdd(batchGzipSize);
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
            boolean zipped = true;
            if (columnIndexItem == null) {
                // 半包批次
                columnIndexItem = new IntIndexItem(Math.toIntExact(batchNum), batchPos, batchSize, batchSum, batchMax);
                zipped = false;
            }
            ByteBuffer byteBuffer = read(columnIndexItem.getPos(), columnIndexItem.getSize());
            if (zipped) {
                byteBuffer = ByteBuffer.wrap(columnOutput.unGZip(byteBuffer.array()));
            }
            int pos = 0;
            int last = byteBuffer.getInt();
            long itemNum = batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos;
            if (batchTimeItemSetMap.get(batchNum).contains(itemNum)) {
                columnItemList.add(new ColumnItem<>(new ColumnValue.IntegerColumn(last), itemNum));
            }
            pos++;
            int lastPre = last;
            last = byteBuffer.getInt();
            itemNum = batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos;
            if (batchTimeItemSetMap.get(batchNum).contains(itemNum)) {
                columnItemList.add(new ColumnItem<>(new ColumnValue.IntegerColumn(last), itemNum));
            }
            pos++;
            while (byteBuffer.remaining() > 0) {
                int cur = last - lastPre + last + columnOutput.readZInt(byteBuffer);
                itemNum = batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos;
                if (batchTimeItemSetMap.get(batchNum).contains(itemNum)) {
                    columnItemList.add(new ColumnItem<>(new ColumnValue.IntegerColumn(cur), itemNum));
                }
                pos++;
                lastPre = last;
                last = cur;
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
                IntIndexItem columnIndexItem = (IntIndexItem) columnIndexItemMap.get(batchNum);
                boolean zipped = true;
                if (columnIndexItem == null) {
                    // 半包批次
                    columnIndexItem = new IntIndexItem(Math.toIntExact(batchNum), batchPos, batchSize, batchSum, batchMax);
                    zipped = false;
                    validCount += batchItemCount;
                } else {
                    validCount += LsmStorage.MAX_ITEM_CNT_L0;
                }

                sum += columnIndexItem.getBatchSum();
                max = Math.max(max, columnIndexItem.getBatchMax());

//                ByteBuffer byteBuffer = read(columnIndexItem.getPos(), columnIndexItem.getSize());
//                if (zipped) {
//                    byteBuffer = ByteBuffer.wrap(columnOutput.unGZip(byteBuffer.array()));
//                }
//                int last = byteBuffer.getInt();
//                sum += last;
//                validCount++;
//                max = Math.max(max, last);
//
//                int lastPre = last;
//                last = byteBuffer.getInt();
//                sum += last;
//                validCount++;
//                max = Math.max(max, last);
//                while (byteBuffer.remaining() > 0) {
//                    int cur = last - lastPre + last + columnOutput.readZInt(byteBuffer);
//                    sum += cur;
//                    validCount++;
//                    max = Math.max(max, cur);
//                    lastPre = last;
//                    last = cur;
//                }
            }
            AGG_HIT_IDX_CNT.getAndAdd(validCount);
        }

        List<Long> batchNumList = batchTimeItemSetMap.keySet().stream().sorted().collect(Collectors.toList());
        for (Long batchNum : batchNumList) {
            ColumnIndexItem columnIndexItem = columnIndexItemMap.get(batchNum);
            boolean zipped = true;
            if (columnIndexItem == null) {
                // 半包批次
                columnIndexItem = new IntIndexItem(Math.toIntExact(batchNum), batchPos, batchSize, batchSum, batchMax);
                zipped = false;
            }
            ByteBuffer byteBuffer = read(columnIndexItem.getPos(), columnIndexItem.getSize());
            if (zipped) {
                byteBuffer = ByteBuffer.wrap(columnOutput.unGZip(byteBuffer.array()));
            }
            int pos = 0;
            int last = byteBuffer.getInt();
            long itemNum = batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos;
            if (batchTimeItemSetMap.get(batchNum).contains(itemNum) && (columnFilter == null || columnFilter.doCompare(new ColumnValue.IntegerColumn(last)))) {
                sum += last;
                validCount++;
                max = Math.max(max, last);
            }
            pos++;
            int lastPre = last;
            last = byteBuffer.getInt();
            itemNum = batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos;
            if (batchTimeItemSetMap.get(batchNum).contains(itemNum) && (columnFilter == null || columnFilter.doCompare(new ColumnValue.IntegerColumn(last)))) {
                sum += last;
                validCount++;
                max = Math.max(max, last);
            }
            pos++;
            while (byteBuffer.remaining() > 0) {
                int cur = last - lastPre + last + columnOutput.readZInt(byteBuffer);
                itemNum = batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos;
                if (batchTimeItemSetMap.get(batchNum).contains(itemNum) && (columnFilter == null || columnFilter.doCompare(new ColumnValue.IntegerColumn(cur)))) {
                    sum += cur;
                    validCount++;
                    max = Math.max(max, cur);
                }
                lastPre = last;
                last = cur;
                pos++;
            }
        }

        AGG_CNT.addAndGet(validCount);
        if (AGG_CNT.get() > 100_000L * AGG_LOG_CNT.get()) {
            if (AGG_LOG_CNT.compareAndSet(AGG_LOG_CNT.get(), AGG_LOG_CNT.get() + 1)) {
                System.out.println("agg count:" + AGG_CNT.get() + ", agg hit idx count:" + AGG_HIT_IDX_CNT.get() + " rate:" + (double) AGG_HIT_IDX_CNT.get() / AGG_CNT.get());
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
