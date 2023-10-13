package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.structs.Aggregator;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class DoubleChannel extends ColumnChannel<ColumnValue.DoubleFloatColumn> {

    public static final AtomicLong ORIG_SIZE = new AtomicLong(0);
    public static final AtomicLong REAL_SIZE = new AtomicLong(0);

    private static final int FULL_BATCH_SIZE = (LsmStorage.MAX_ITEM_CNT_L0 - 1) * 8 + 8;

    private static final int TMP_IDX_SIZE = 4 + 8 + 4 + 8 + 8 + 4;

    public static final int IDX_SIZE = 8 + 8 + 8 + 4 + 4;

    private double batchSum;

    private double batchMax;

    private int batchMaxScale;

    private transient boolean halfBatchRecovered;

    public DoubleChannel(File vinDir, TableSchema.Column column) throws IOException {
        super(vinDir, column);
    }

    @Override
    public void append0(ColumnValue.DoubleFloatColumn doubleFloatColumn) throws IOException {
        double v = doubleFloatColumn.getDoubleFloatValue();
        batchMaxScale = Math.max(batchMaxScale, new BigDecimal(String.valueOf(v)).scale());
        columnOutput.writeDouble(v);
        batchSum += v;
        batchMax = Math.max(batchMax, v);

        ORIG_SIZE.getAndAdd(8);
    }

    @Override
    public ColumnIndexItem readColumnIndexItem(ByteBuffer byteBuffer) throws IOException {
        double batchSum = byteBuffer.getDouble();
        double batchMax = byteBuffer.getDouble();
        long batchPos = byteBuffer.getLong();
        int batchSize = byteBuffer.getInt();
        int batchMaxScale = byteBuffer.getInt();
        DoubleIndexItem doubleIndexItem = new DoubleIndexItem(-1, batchPos, batchSize, batchSum, batchMax, batchMaxScale);
        return doubleIndexItem;
    }

    @Override
    protected void index(DataChannel columnIndexChannel, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException {
        flush();
        int batchCompressSize;
        if (batchMaxScale > 6) {
            batchCompressSize = columnOutput.batchElfForDouble(batchPos, batchSize);
        } else {
            batchCompressSize = columnOutput.batchElfForDoubleV2(batchPos, batchSize, batchMaxScale);
        }

        columnIndexChannel.writeDouble(batchSum);
        columnIndexChannel.writeDouble(batchMax);
        columnIndexChannel.writeLong(batchPos);
        columnIndexChannel.writeInt(batchCompressSize);
        columnIndexChannel.writeInt(batchMaxScale);

        columnIndexItemMap.put((long) columnIndexItemMap.size(), new DoubleIndexItem(-1, batchPos, batchCompressSize, batchSum, batchMax, batchMaxScale));

        batchSum = 0;
        batchMax = -Double.MAX_VALUE;
        batchMaxScale = 0;

        REAL_SIZE.getAndAdd(batchCompressSize);
    }

    @Override
    protected void noNeedRecoverTmpIndex() throws IOException {
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
        batchPos = byteBuffer.getLong();
        batchSize = byteBuffer.getInt();
        batchSum = byteBuffer.getDouble();
        batchMax = byteBuffer.getDouble();
        batchMaxScale = byteBuffer.getInt();

        halfBatchRecovered = true;
    }

    @Override
    protected void shutdownTmpIndex() throws IOException {
        if (columnOutput.isDirty) {
            // todo 半包标记 目前shutdown后不会再写
            flush();
            int batchCompressSize;
            if (batchMaxScale > 6) {
                batchCompressSize = columnOutput.batchElfForDouble(batchPos, batchSize);
            } else {
                batchCompressSize = columnOutput.batchElfForDoubleV2(batchPos, batchSize, batchMaxScale);
            }
            batchSize = batchCompressSize;
            REAL_SIZE.getAndAdd(batchCompressSize);
        }

        FileOutputStream fileOutputStream = new FileOutputStream(tmpIndexFile, false);
        ByteBuffer byteBuffer = ByteBuffer.allocate(TMP_IDX_SIZE);
        byteBuffer.putInt(batchItemCount);
        byteBuffer.putLong(batchPos);
        byteBuffer.putInt(batchSize);
        byteBuffer.putDouble(batchSum);
        byteBuffer.putDouble(batchMax);
        byteBuffer.putInt(batchMaxScale);
        fileOutputStream.write(byteBuffer.array());
        fileOutputStream.flush();
        fileOutputStream.close();
    }

    @Override
    protected int batchGrow(ColumnValue.DoubleFloatColumn doubleFloatColumn) throws IOException {
        return 8;
    }

    @Override
    protected List<DoubleIndexItem> loadAllIndex() throws IOException {
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
//        List<DoubleIndexItem> indexItemList = new ArrayList<>(indexItemCount);
//
//        if (indexItemCount > 0) {
//            indexItemList.add(new DoubleIndexItem(0, 0, FULL_BATCH_SIZE, byteBuffer.getDouble(), byteBuffer.getDouble()));
//        }
//
//        for (int i = 1; i < indexItemCount; i++) {
//            DoubleIndexItem indexItem = indexItemList.get(i - 1);
//            indexItemList.add(new DoubleIndexItem(i, indexItem.getPos() + indexItem.getSize(), FULL_BATCH_SIZE, byteBuffer.getDouble(), byteBuffer.getDouble()));
//        }
//
//        if (batchItemCount > 0) {
//            if (indexItemCount > 0) {
//                DoubleIndexItem indexItem = indexItemList.get(indexItemCount - 1);
//                indexItemList.add(new DoubleIndexItem(indexItemCount, indexItem.getPos() + indexItem.getSize(), batchSize, batchSum, batchMax));
//            } else {
//                indexItemList.add(new DoubleIndexItem(indexItemCount, 0, batchSize, batchSum, batchMax));
//            }
//        }
//        return indexItemList;
        return null;
    }

    @Override
    public List<ColumnItem<ColumnValue.DoubleFloatColumn>> range(List<TimeItem> timeItemList, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException {
        super.flush();

        List<ColumnItem<ColumnValue.DoubleFloatColumn>> columnItemList = new ArrayList<>(timeItemList.size());

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
                columnIndexItem = new DoubleIndexItem(Math.toIntExact(batchNum), batchPos, batchSize, batchSum, batchMax, batchMaxScale);
                if (!halfBatchRecovered) {
                    // 临时半包，没有就进行压缩，写入时每百万条读取抽查
                    zipped = false;
                }
            }
            ByteBuffer byteBuffer = read(columnIndexItem.getPos(), columnIndexItem.getSize());
            if (zipped) {
                if (batchMaxScale > 6) {
                    byteBuffer = ByteBuffer.wrap(columnOutput.batchUnElfForDouble(byteBuffer));
                } else {
                    byteBuffer = ByteBuffer.wrap(columnOutput.batchUnElfForDoubleV2(byteBuffer, batchMaxScale));
                }
            }
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
    public ColumnValue.DoubleFloatColumn agg(List<TimeItem> batchItemList, List<TimeItem> timeItemList, Aggregator aggregator,
                                             CompareExpression columnFilter, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException {
        double sum = 0.0;
        double max = -Double.MAX_VALUE;
        int validCount = 0;

        Map<Long, Set<Long>> batchTimeItemSetMap = new HashMap<>();
        for (TimeItem timeItem : timeItemList) {
            if (timeItem.getTime() > 0) {
                Set<Long> timeItemSet = batchTimeItemSetMap.computeIfAbsent(timeItem.getBatchNum(), v -> new HashSet<>());
                timeItemSet.add(timeItem.getItemNum());
            }
        }

        if (!batchTimeItemSetMap.isEmpty()) {
            // 需要扫描数据列
//            super.flush();
        }

        if (columnFilter == null && !batchItemList.isEmpty()) {
            for (TimeItem item : batchItemList) {
                long batchNum = item.getBatchNum();
                DoubleIndexItem columnIndexItem = (DoubleIndexItem) columnIndexItemMap.get(batchNum);
                boolean zipped = true;
                if (columnIndexItem == null) {
                    // 半包批次
                    columnIndexItem = new DoubleIndexItem(Math.toIntExact(batchNum), batchPos, batchSize, batchSum, batchMax, batchMaxScale);
                    if (!halfBatchRecovered) {
                        // 临时半包，没有就进行压缩，写入时每百万条读取抽查
                        zipped = false;
                    }
                    validCount += batchItemCount;
                } else {
                    validCount += LsmStorage.MAX_ITEM_CNT_L0;
                }

                sum += columnIndexItem.getBatchSum();
                max = Math.max(max, columnIndexItem.getBatchMax());

//                ByteBuffer byteBuffer = read(columnIndexItem.getPos(), columnIndexItem.getSize());
//                double last = byteBuffer.getDouble();
//                sum += last;
//                validCount++;
//                max = Math.max(last, max);
//                while (byteBuffer.remaining() > 0) {
//                    last = byteBuffer.getDouble();
//                    sum += last;
//                    validCount++;
//                    max = Math.max(last, max);
//                }
            }
        }

        List<Long> batchNumList = batchTimeItemSetMap.keySet().stream().sorted().collect(Collectors.toList());
        for (Long batchNum : batchNumList) {
            ColumnIndexItem columnIndexItem = columnIndexItemMap.get(batchNum);
            boolean zipped = true;
            if (columnIndexItem == null) {
                // 半包批次
                columnIndexItem = new DoubleIndexItem(Math.toIntExact(batchNum), batchPos, batchSize, batchSum, batchMax, batchMaxScale);
                if (!halfBatchRecovered) {
                    // 临时半包，没有就进行压缩，写入时每百万条读取抽查
                    zipped = false;
                }
            }
            ByteBuffer byteBuffer = read(columnIndexItem.getPos(), columnIndexItem.getSize());
            if (zipped) {
                if (batchMaxScale > 6) {
                    byteBuffer = ByteBuffer.wrap(columnOutput.batchUnElfForDouble(byteBuffer));
                } else {
                    byteBuffer = ByteBuffer.wrap(columnOutput.batchUnElfForDoubleV2(byteBuffer, batchMaxScale));
                }
            }
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
