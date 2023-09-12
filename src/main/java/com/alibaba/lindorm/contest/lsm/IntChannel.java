package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.structs.ColumnValue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class IntChannel extends ColumnChannel<ColumnValue.IntegerColumn> {

    private static int FULL_BATCH_SIZE = (LsmStorage.MAX_ITEM_CNT_L0 - 1) * 4 + 4;

    private final FileChannel columnInput;

    public IntChannel(File vinDir, TableSchema.Column column) throws IOException {
        super(vinDir, column);

        columnInput = new RandomAccessFile(columnFile, "r").getChannel();
    }

    @Override
    public void append(ColumnValue.IntegerColumn integerColumn) throws IOException {
        CommonUtils.writeInt(columnOutput, integerColumn.getIntegerValue());
    }


    @Override
    public List<ColumnItem<ColumnValue.IntegerColumn>> range(List<TimeItem> timeItemList) throws IOException {
        List<ColumnItem<ColumnValue.IntegerColumn>> columnItemList = new ArrayList<>(timeItemList.size());

        Map<Long, Set<TimeItem>> batchTimeItemSetMap = new HashMap<>();
        for (TimeItem timeItem : timeItemList) {
            Set<TimeItem> timeItemSet = batchTimeItemSetMap.computeIfAbsent(timeItem.getBatchNum(), v -> new HashSet<>());
            timeItemSet.add(timeItem);
        }

        List<Long> batchNumList = timeItemList.stream().map(TimeItem::getBatchNum).collect(Collectors.toSet()).stream().sorted().collect(Collectors.toList());
        for (Long batchNum : batchNumList) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(FULL_BATCH_SIZE);
            columnInput.read(byteBuffer, (long) batchNum * FULL_BATCH_SIZE);
            int pos = 0;
            int last = byteBuffer.getInt();
            long itemNum = batchNum * FULL_BATCH_SIZE + pos;
            if (batchTimeItemSetMap.get(batchNum).contains(itemNum)) {
                columnItemList.add(new ColumnItem<>(new ColumnValue.IntegerColumn(last), itemNum));
            }
            pos++;
            while (byteBuffer.remaining() > 0) {
                last = byteBuffer.getInt();
                itemNum = batchNum * FULL_BATCH_SIZE + pos;
                if (batchTimeItemSetMap.get(batchNum).contains(itemNum)) {
                    columnItemList.add(new ColumnItem<>(new ColumnValue.IntegerColumn(last), itemNum));
                }
                pos++;
            }
        }

        return columnItemList;
    }
}
