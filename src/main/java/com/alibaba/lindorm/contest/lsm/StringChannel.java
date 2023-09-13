package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.structs.ColumnValue;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.stream.Collectors;

public class StringChannel extends ColumnChannel<ColumnValue.StringColumn> {

    private final OutputStream indexOutput;

    private final FileChannel indexInput;

    private int batchSize;

    private long batchItemCount;

    public StringChannel(File vinDir, TableSchema.Column column) throws IOException {
        super(vinDir, column);

        File idxFile = new File(vinDir.getAbsolutePath(), column.columnName + ".idx");
        if (!idxFile.exists()) {
            idxFile.createNewFile();
        }
        indexOutput = new BufferedOutputStream(new FileOutputStream(idxFile, true));
        indexInput = new RandomAccessFile(idxFile, "r").getChannel();
    }

    @Override
    public void append(ColumnValue.StringColumn stringColumn) throws IOException {
        // todo 批压缩
        CommonUtils.writeString(columnOutput, stringColumn.getStringValue());
        batchItemCount++;
        batchSize += 4 + stringColumn.getStringValue().limit();
    }

    public boolean shutdownAndIndex() throws IOException {
        if (batchItemCount <= 0) {
            return false;
        }

        batchIndex();
        return true;
    }

    private void batchIndex() throws IOException {
        CommonUtils.writeInt(indexOutput, batchSize);
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
    public List<ColumnItem<ColumnValue.StringColumn>> range(List<TimeItem> timeItemList) throws IOException {
        List<ColumnItem<ColumnValue.StringColumn>> columnItemList = new ArrayList<>(timeItemList.size());

        Map<Long, Set<Long>> batchTimeItemSetMap = new HashMap<>();
        for (TimeItem timeItem : timeItemList) {
            Set<Long> timeItemSet = batchTimeItemSetMap.computeIfAbsent(timeItem.getBatchNum(), v -> new HashSet<>());
            timeItemSet.add(timeItem.getBatchNum());
        }

        MappedByteBuffer byteBuffer = indexInput.map(FileChannel.MapMode.READ_ONLY, 0, indexInput.size());
        if (byteBuffer.limit() != indexInput.size()) {
            throw new IllegalStateException("全量读取稀疏索引失败");
        }
        if (byteBuffer.limit() % 4 != 0) {
            throw new IllegalStateException("稀疏索引文件损坏");
        }
        int indexItemCount = byteBuffer.limit() / 4;
        long[] batchPoss = new long[indexItemCount];
        int[] batchSizes = new int[indexItemCount];
        batchPoss[0] = 0;
        batchSizes[0] = byteBuffer.getInt();
        for (int i = 1; i < indexItemCount; i++) {
            batchPoss[i] = batchPoss[i - 1] + batchSizes[i - 1];
            batchSizes[i] = byteBuffer.getInt();
        }

        List<Long> batchNumList = batchTimeItemSetMap.keySet().stream().sorted().collect(Collectors.toList());
        for (Long batchNum : batchNumList) {
            columnItemList.addAll(range(batchNum, batchPoss[batchNum.intValue()], batchSizes[batchNum.intValue()], batchTimeItemSetMap.get(batchNum)));
        }
        return columnItemList;
    }

    private List<ColumnItem<ColumnValue.StringColumn>> range(long batchNum, long pos, int size, Set<Long> batchItemSet) throws IOException {
        List<ColumnItem<ColumnValue.StringColumn>> columnItemList = new ArrayList<>();

        ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        columnInput.read(byteBuffer, pos);

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
}
