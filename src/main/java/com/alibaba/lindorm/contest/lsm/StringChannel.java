package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.structs.Aggregator;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;
import com.alibaba.lindorm.contest.util.ByteBufferUtil;
import com.alibaba.lindorm.contest.util.NumberUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class StringChannel extends ColumnChannel<ColumnValue.StringColumn> {

    public static final int IDX_SIZE = 8 + 4 + 4;

    public static final AtomicLong ORIG_SIZE = new AtomicLong(0);
    public static final AtomicLong REAL_SIZE = new AtomicLong(0);

    public StringChannel(File vinDir, TableSchema.Column column, File columnFile, DataChannel columnOutput) throws IOException {
        super(vinDir, column, columnFile, columnOutput);
    }

    @Override
    protected void append0(List<ColumnValue.StringColumn> stringColumns) throws IOException {
        int size = 0;
        int origSize = 0;
        // 488288
        Map<ByteBuffer, Integer> map = new LinkedHashMap<>(stringColumns.size() >> 2);
        List<Integer> ints = new ArrayList<>(stringColumns.size());
        for (ColumnValue.StringColumn stringColumn : stringColumns) {
            Integer integer = map.get(stringColumn.getStringValue());
            if (integer != null) {
                ints.add(integer);
            } else {
                ints.add(map.size());
                map.put(stringColumn.getStringValue(), map.size());
                size += 4 + stringColumn.getStringValue().limit();
            }
            origSize += 4 + stringColumn.getStringValue().limit();
        }
        ByteBuffer zInt = NumberUtil.zInt(ints);
        ByteBuffer buffer = ByteBuffer.allocate(4 + zInt.limit() + size);
        buffer.putInt(zInt.limit());
        buffer.put(zInt);
        for (ByteBuffer byteBuffer : map.keySet()) {
            buffer.putInt(byteBuffer.limit());
            buffer.put(byteBuffer);
        }
        byte[] bytes = ByteBufferUtil.zstdEncode(buffer.array());
        batchSize = bytes.length;
        columnOutput.writeBytes(bytes);
        ORIG_SIZE.getAndAdd(origSize);
    }

    @Override
    protected void index(DataChannel columnIndexChannel, Map<Long, ColumnIndexItem> columnIndexItemMap) throws IOException {
        columnIndexChannel.writeLong(batchPos);
        columnIndexChannel.writeInt(batchSize);
        columnIndexChannel.writeInt(batchItemCount);

        columnIndexItemMap.put((long) columnIndexItemMap.size(), new StringIndexItem(-1, batchPos, batchSize, batchItemCount));

        REAL_SIZE.getAndAdd(batchSize);
    }

    @Override
    public ColumnIndexItem readColumnIndexItem(ByteBuffer byteBuffer) throws IOException {
        long batchPos = byteBuffer.getLong();
        int batchSize = byteBuffer.getInt();
        int batchItemCount = byteBuffer.getInt();
        StringIndexItem stringIndexItem = new StringIndexItem(-1, batchPos, batchSize, batchItemCount);
        return stringIndexItem;
    }

    @Override
    public ColumnValue.StringColumn agg(List<TimeItem> batchItemList, List<TimeItem> timeItemList, Aggregator aggregator,
                                        CompareExpression columnFilter, Map<Long, ColumnIndexItem> columnIndexItemMap, List<ColumnValue.StringColumn> notcheckList) throws IOException {
        throw new IllegalStateException("string类型不支持聚合");
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

        List<Long> batchNumList = batchTimeItemSetMap.keySet().stream().sorted().collect(Collectors.toList());
        for (Long batchNum : batchNumList) {
            ColumnIndexItem columnIndexItem = columnIndexItemMap.get(batchNum);
            columnItemList.addAll(range(batchNum, columnIndexItem.getPos(), columnIndexItem.getSize(), batchTimeItemSetMap.get(batchNum)));
        }
        return columnItemList;
    }

    @Override
    public void flush() throws IOException {
//        indexOutput.flush();
        super.flush();
    }

    private List<ColumnItem<ColumnValue.StringColumn>> range(long batchNum, long pos, int size, Set<Long> batchItemSet) throws IOException {
        flush();

        List<ColumnItem<ColumnValue.StringColumn>> columnItemList = new ArrayList<>();

        ByteBuffer byteBuffer = read(pos, size);
        byteBuffer = ByteBuffer.wrap(ByteBufferUtil.zstdDecode(byteBuffer));
        int intSize = byteBuffer.getInt();
        ByteBuffer slice = byteBuffer.slice();
        slice.limit(intSize);
        List<Integer> ints = NumberUtil.rzInt(slice);
        byteBuffer.position(4 + intSize);
        Map<Integer, ColumnValue.StringColumn> map = new HashMap<>(ints.size() >> 2);
        int posInBatch = 0;
        do {
            ColumnValue.StringColumn column = readFrom(byteBuffer);
            map.put(posInBatch, column);
            posInBatch++;
        } while (byteBuffer.hasRemaining());

        for (int i = 0; i < ints.size(); i++) {
            long itemNum = i + LsmStorage.MAX_ITEM_CNT_L0 * batchNum;
            if (batchItemSet.contains(itemNum)) {
                columnItemList.add(new ColumnItem<>(map.get(ints.get(i)), itemNum));
            }
        }

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
//        indexOutput.flush();
//        indexOutput.close();
        super.shutdown();
    }
}
