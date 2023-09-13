package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.CommonUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class TimeChannel {

    private static int FULL_BATCH_SIZE = (LsmStorage.MAX_ITEM_CNT_L0 - 1) * 4 + 8;

    private final OutputStream timeOutput;

    private final OutputStream timeIndexOutput;

    private final FileChannel timeInput;

    private final FileChannel timeIndexInput;

    private long minTime;

    private long maxTime;

    private long lastTime;

    private long batchItemCount;

    public TimeChannel(File vinDir) throws IOException {
        File timeFile = new File(vinDir.getAbsolutePath(), "time");
        if (!timeFile.exists()) {
            timeFile.createNewFile();
        }
        timeOutput = new BufferedOutputStream(new FileOutputStream(timeFile, true));

        File timeIdxFile = new File(vinDir.getAbsolutePath(), "time.idx");
        if (!timeIdxFile.exists()) {
            timeIdxFile.createNewFile();
        }
        timeIndexOutput = new BufferedOutputStream(new FileOutputStream(timeIdxFile, true));

        timeInput = new RandomAccessFile(timeFile, "r").getChannel();
        timeIndexInput = new RandomAccessFile(timeFile, "r").getChannel();
    }

    public void append(long time) throws IOException {
        if (batchItemCount == 0) {
            minTime = time;
            maxTime = time;
            lastTime = time;
            CommonUtils.writeLong(timeOutput, time);
            batchItemCount++;
            return;
        }
        minTime = Math.min(minTime, time);
        maxTime = Math.min(maxTime, time);
        // todo 变长编码
        CommonUtils.writeInt(timeOutput, (int) (time - lastTime));
        batchItemCount++;
    }

    public boolean shutdownAndIndex() throws IOException {
        if (batchItemCount <= 0) {
            return false;
        }

        index();

        batchItemCount = 0;
        return true;
    }

    private void index() throws IOException {
        // 输出主键稀疏索引 todo maxTime_delta, delta_bf
        TimeIndexItem timeIndexItem = new TimeIndexItem(minTime, maxTime);
        CommonUtils.writeLong(timeIndexOutput, timeIndexItem.getMinTime());
        CommonUtils.writeLong(timeIndexOutput, timeIndexItem.getMaxTime());
    }

    /**
     * append后调用
     *
     * @return
     * @throws IOException
     */
    public boolean checkAndIndex() throws IOException {
        if (batchItemCount < LsmStorage.MAX_ITEM_CNT_L0) {
            return false;
        }

        index();

        batchItemCount = 0;
        return true;
    }

    public List<TimeItem> range(long l, long r) throws IOException {
        List<TimeItem> timeItemList = new ArrayList<>();

        MappedByteBuffer byteBuffer = timeIndexInput.map(FileChannel.MapMode.READ_ONLY, 0, timeIndexInput.size());
        if (byteBuffer.limit() != timeIndexInput.size()) {
            throw new IllegalStateException("全量读取主键稀疏索引失败");
        }
        if (byteBuffer.limit() % TimeIndexItem.SIZE != 0) {
            throw new IllegalStateException("主键稀疏索引文件损坏");
        }
        int indexItemCount = byteBuffer.limit() / TimeIndexItem.SIZE;

        for (int i = 0; i < indexItemCount; i++) {
            long minTime = byteBuffer.getLong();
            long maxTime = byteBuffer.getLong();
            if (l < maxTime || r <= minTime) {
                continue;
            }
            // 需要扫描这一批次
            timeItemList.addAll(range(l, r, i));
        }
        return timeItemList;
    }

    private List<TimeItem> range(long l, long r, int batchNum) throws IOException {
        List<TimeItem> timeItemList = new ArrayList<>();

        ByteBuffer byteBuffer = ByteBuffer.allocate(FULL_BATCH_SIZE);
        timeInput.read(byteBuffer, (long) batchNum * FULL_BATCH_SIZE);
        int pos = 0;
        long last = byteBuffer.getLong();
        if (last >= l && last < r) {
            timeItemList.add(new TimeItem(last, (long) batchNum * FULL_BATCH_SIZE + pos));
        }
        pos++;
        while (byteBuffer.remaining() > 0) {
            last = last + byteBuffer.getInt();
            if (last >= l && last < r) {
                timeItemList.add(new TimeItem(last, (long) batchNum * FULL_BATCH_SIZE + pos));
            }
            pos++;
        }
        return timeItemList;
    }
}
