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

    private static final int TMP_IDX_SIZE = 4 + 8 + 4;

    public static final int IDX_SIZE = 8 + 4;

    private final File indexFile;

    private final OutputStream indexOutput;

    public StringChannel(File vinDir, TableSchema.Column column) throws IOException {
        super(vinDir, column);
        indexFile = new File(vinDir.getAbsolutePath(), column.columnName + ".idx");
        if (!indexFile.exists()) {
            indexFile.createNewFile();
        }
        indexOutput = new BufferedOutputStream(new FileOutputStream(indexFile, true), LsmStorage.OUTPUT_BUFFER_SIZE);

    }

    @Override
    protected void shutdownTmpIndex() throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(tmpIndexFile, false);
        ByteBuffer byteBuffer = ByteBuffer.allocate(TMP_IDX_SIZE);
        byteBuffer.putInt(batchItemCount);
        byteBuffer.putLong(batchPos);
        byteBuffer.putInt(batchSize);
        fileOutputStream.write(byteBuffer.array());
        fileOutputStream.flush();
        fileOutputStream.close();
    }

    @Override
    protected void noNeedRecoverTmpIndex() throws IOException {

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
    }

    @Override
    protected void append0(ColumnValue.StringColumn stringColumn) throws IOException {
        // todo 批压缩
        columnOutput.writeString(stringColumn.getStringValue());
    }

    @Override
    protected int batchGrow(ColumnValue.StringColumn stringColumn) throws IOException {
        return 4 + stringColumn.getStringValue().limit();
    }

    @Override
    protected void index(DataChannel columnIndexChannel) throws IOException {
        columnIndexChannel.writeLong(batchPos);
        columnIndexChannel.writeInt(batchSize);
    }

    @Override
    public ColumnIndexItem readColumnIndexItem(ByteBuffer byteBuffer) throws IOException {
        long batchPos = byteBuffer.getLong();
        int batchSize = byteBuffer.getInt();
        StringIndexItem stringIndexItem = new StringIndexItem(-1, batchPos, batchSize);
        return stringIndexItem;
    }

    @Override
    public ColumnValue.StringColumn agg(List<TimeItem> batchItemList, List<TimeItem> timeItemList, Aggregator aggregator,
                                        CompareExpression columnFilter, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException {
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

    @Override
    protected List<StringIndexItem> loadAllIndex() throws IOException {
        FileInputStream fileInputStream = new FileInputStream(indexFile);
        ByteBuffer byteBuffer = ByteBuffer.allocate((int) indexFile.length());
        if (fileInputStream.read(byteBuffer.array()) != (int) indexFile.length()) {
            throw new IllegalStateException("稀疏索引文件损坏");
        }
        fileInputStream.close();

        if (byteBuffer.limit() % IDX_SIZE != 0) {
            throw new IllegalStateException("稀疏索引文件损坏");
        }
        int indexItemCount = byteBuffer.limit() / IDX_SIZE;
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
    public List<ColumnItem<ColumnValue.StringColumn>> range(List<TimeItem> timeItemList, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException {
        flush();

        List<ColumnItem<ColumnValue.StringColumn>> columnItemList = new ArrayList<>(timeItemList.size());

        Map<Long, Set<Long>> batchTimeItemSetMap = new HashMap<>();
        for (TimeItem timeItem : timeItemList) {
            Set<Long> timeItemSet = batchTimeItemSetMap.computeIfAbsent(timeItem.getBatchNum(), v -> new HashSet<>());
            timeItemSet.add(timeItem.getItemNum());
        }

//        List<StringIndexItem> indexItemList = loadAllIndex();

        List<Long> batchNumList = batchTimeItemSetMap.keySet().stream().sorted().collect(Collectors.toList());
        for (Long batchNum : batchNumList) {
            ColumnIndexItem columnIndexItem = columnIndexItemMap.get(batchNum);
            if (columnIndexItem == null) {
                // 半包批次
                columnIndexItem = new StringIndexItem(Math.toIntExact(batchNum), batchPos, batchSize);
            }
            columnItemList.addAll(range(batchNum, columnIndexItem.getPos(), columnIndexItem.getSize(), batchTimeItemSetMap.get(batchNum)));
        }
        return columnItemList;
    }

    @Override
    public void flush() throws IOException {
        indexOutput.flush();
        super.flush();
    }

    private List<ColumnItem<ColumnValue.StringColumn>> range(long batchNum, long pos, int size, Set<Long> batchItemSet) throws IOException {
        flush();

        List<ColumnItem<ColumnValue.StringColumn>> columnItemList = new ArrayList<>();

        ByteBuffer byteBuffer = read(pos, size);

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
        indexOutput.flush();
        indexOutput.close();
        super.shutdown();
    }
}
