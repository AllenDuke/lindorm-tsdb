package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.structs.Aggregator;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.stream.Collectors;

public class StringChannel extends ColumnChannel<ColumnValue.StringColumn> {

    private final OutputStream indexOutput;

    private final FileChannel indexInput;

    /**
     * 作用于shutdown时，没有满一批。
     */
    private final FileChannel tmpIndexChannel;

    private long indexFileSize;

    private int batchSize;

    private int batchItemCount;

    public StringChannel(File vinDir, TableSchema.Column column) throws IOException {
        super(vinDir, column);

        File idxFile = new File(vinDir.getAbsolutePath(), column.columnName + ".idx");
        if (!idxFile.exists()) {
            idxFile.createNewFile();
        }
        indexOutput = new BufferedOutputStream(new FileOutputStream(idxFile, true));
        indexInput = new RandomAccessFile(idxFile, "r").getChannel();
        indexFileSize = idxFile.length();

        File tmpIdxFile = new File(vinDir.getAbsolutePath(), column.columnName + ".tmp");
        if (!tmpIdxFile.exists()) {
            tmpIdxFile.createNewFile();
        }
        tmpIndexChannel = new RandomAccessFile(tmpIdxFile, "rw").getChannel();
        if (tmpIndexChannel.size() != 0) {
            MappedByteBuffer byteBuffer = tmpIndexChannel.map(FileChannel.MapMode.READ_ONLY, 0, 4 + 4);
            batchItemCount = byteBuffer.getInt();
            batchSize = byteBuffer.getInt();
        }
    }

    @Override
    public void append(ColumnValue.StringColumn stringColumn) throws IOException {
        // todo 批压缩
        CommonUtils.writeString(columnOutput, stringColumn.getStringValue());
        batchItemCount++;
        batchSize += 4 + stringColumn.getStringValue().limit();

        checkAndIndex();
    }

    private void batchIndex() throws IOException {
        CommonUtils.writeInt(indexOutput, batchSize);
        indexFileSize += 4;
        batchItemCount = 0;
        batchSize = 0;
    }

    public boolean checkAndIndex() throws IOException {
        if (batchItemCount < LsmStorage.MAX_ITEM_CNT_L0) {
            return false;
        }

        batchIndex();
        return true;
    }

    @Override
    public ColumnValue.StringColumn agg(List<TimeItem> timeItemList, Aggregator aggregator, CompareExpression columnFilter) throws IOException {
        throw new IllegalStateException("string类型不支持聚合");
//        Map<Long, Set<Long>> batchTimeItemSetMap = new HashMap<>();
//        for (TimeItem timeItem : timeItemList) {
//            Set<Long> timeItemSet = batchTimeItemSetMap.computeIfAbsent(timeItem.getBatchNum(), v -> new HashSet<>());
//            timeItemSet.add(timeItem.getItemNum());
//        }
//
//        List<StringIndexItem> indexItemList = loadAllIndex();
//
//        List<Long> batchNumList = batchTimeItemSetMap.keySet().stream().sorted().collect(Collectors.toList());
//        for (Long batchNum : batchNumList) {
//            ByteBuffer byteBuffer = ByteBuffer.allocate(indexItemList.get(batchNum.intValue()).getSize());
//            columnInput.read(byteBuffer, indexItemList.get(batchNum.intValue()).getPos());
//            byteBuffer.flip();
//
//            int posInBatch = 0;
//            do {
//                ColumnValue.StringColumn column = readFrom(byteBuffer);
//                long itemNum = posInBatch + LsmStorage.MAX_ITEM_CNT_L0 * batchNum;
//                if (batchTimeItemSetMap.get(batchNum).contains(itemNum) && columnFilter.doCompare(column)) {
//
//                }
//                posInBatch++;
//            } while (byteBuffer.hasRemaining());
//        }
//        return null;
    }

    private List<StringIndexItem> loadAllIndex() throws IOException {
        MappedByteBuffer byteBuffer = indexInput.map(FileChannel.MapMode.READ_ONLY, 0, indexFileSize);
        if (byteBuffer.limit() != indexInput.size()) {
            throw new IllegalStateException("全量读取稀疏索引失败");
        }
        if (byteBuffer.limit() % 4 != 0) {
            throw new IllegalStateException("稀疏索引文件损坏");
        }
        int indexItemCount = byteBuffer.limit() / 4;
        List<StringIndexItem> indexItemList = new ArrayList<>(indexItemCount);

        if (indexItemCount > 0) {
            indexItemList.add(new StringIndexItem(0, 0, byteBuffer.getInt()));
        }

        for (int i = 1; i < indexItemCount; i++) {
            StringIndexItem indexItem = indexItemList.get(i - 1);
            indexItemList.add(new StringIndexItem(i, indexItem.getPos() + indexItem.getSize(), byteBuffer.getInt()));
        }

        if (batchItemCount > 0) {
            if (indexItemCount > 0) {
                StringIndexItem indexItem = indexItemList.get(indexItemCount - 1);
                indexItemList.add(new StringIndexItem(indexItemCount, indexItem.getPos() + indexItem.getSize(), batchSize));
            } else {
                indexItemList.add(new StringIndexItem(indexItemCount, 0, batchSize));
            }
        }
        return indexItemList;
    }

    @Override
    public List<ColumnItem<ColumnValue.StringColumn>> range(List<TimeItem> timeItemList) throws IOException {
        columnOutput.flush();
        indexOutput.flush();

        List<ColumnItem<ColumnValue.StringColumn>> columnItemList = new ArrayList<>(timeItemList.size());

        Map<Long, Set<Long>> batchTimeItemSetMap = new HashMap<>();
        for (TimeItem timeItem : timeItemList) {
            Set<Long> timeItemSet = batchTimeItemSetMap.computeIfAbsent(timeItem.getBatchNum(), v -> new HashSet<>());
            timeItemSet.add(timeItem.getItemNum());
        }

        List<StringIndexItem> indexItemList = loadAllIndex();

        List<Long> batchNumList = batchTimeItemSetMap.keySet().stream().sorted().collect(Collectors.toList());
        for (Long batchNum : batchNumList) {
            columnItemList.addAll(range(batchNum, indexItemList.get(batchNum.intValue()).getPos(), indexItemList.get(batchNum.intValue()).getSize(), batchTimeItemSetMap.get(batchNum)));
        }
        return columnItemList;
    }

    private List<ColumnItem<ColumnValue.StringColumn>> range(long batchNum, long pos, int size, Set<Long> batchItemSet) throws IOException {
        columnOutput.flush();
        indexOutput.flush();

        List<ColumnItem<ColumnValue.StringColumn>> columnItemList = new ArrayList<>();

        ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        columnInput.read(byteBuffer, pos);
        byteBuffer.flip();

        int posInBatch = 0;
        do {
            ColumnValue.StringColumn column = readFrom(byteBuffer);
            long itemNum = posInBatch + LsmStorage.MAX_ITEM_CNT_L0 * batchNum;
            if (batchItemSet.contains(itemNum)) {
                columnItemList.add(new ColumnItem<>(column, itemNum));
            }
            posInBatch++;
        } while (byteBuffer.hasRemaining());
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
        if (batchItemCount > 0) {
            MappedByteBuffer byteBuffer = tmpIndexChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4 + 8 + 8 + 8);
            byteBuffer.putInt(batchItemCount);
            byteBuffer.putInt(batchSize);
            byteBuffer.force();
        }
        tmpIndexChannel.close();

        indexOutput.flush();
        indexOutput.close();
        indexInput.close();
        super.shutdown();
    }
}
