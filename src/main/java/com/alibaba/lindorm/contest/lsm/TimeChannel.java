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

    private static int TMP_TIME_IDX_SIZE = 4 + 8 + 8 + 8;

    private final OutputStream timeOutput;

    private final OutputStream timeIndexOutput;

    private final FileChannel timeInput;

    private final FileChannel timeIndexInput;

    private long indexFileSize;

    private long minTime;

    private long maxTime;

    private long lastTime;

    private int batchItemCount;

    private final String vinStr;

    /**
     * 主键稀疏索引常驻内存
     */
    private final List<TimeIndexItem> timeIndexItemList = new ArrayList<>();

    private final File tmpTimeIdxFile;

    public TimeChannel(File vinDir) throws IOException {
        File timeFile = new File(vinDir.getAbsolutePath(), "time");
        if (!timeFile.exists()) {
            timeFile.createNewFile();
        }
        timeOutput = new BufferedOutputStream(new FileOutputStream(timeFile, true));

        vinStr = vinDir.getName();

        File timeIdxFile = new File(vinDir.getAbsolutePath(), "time.idx");
        if (!timeIdxFile.exists()) {
            timeIdxFile.createNewFile();
        }
        timeIndexOutput = new BufferedOutputStream(new FileOutputStream(timeIdxFile, true));
        indexFileSize = timeIdxFile.length();

        timeInput = new RandomAccessFile(timeFile, "r").getChannel();
        timeIndexInput = new RandomAccessFile(timeIdxFile, "r").getChannel();
        loadAllIndex();

        tmpTimeIdxFile = new File(vinDir.getAbsolutePath(), "time.tmp");
        if (tmpTimeIdxFile.exists()) {
            // shutdown时未满一批
            byte[] bytes = new byte[TMP_TIME_IDX_SIZE];
            FileInputStream fileInputStream = new FileInputStream(tmpTimeIdxFile);
            int read = fileInputStream.readNBytes(bytes, 0, TMP_TIME_IDX_SIZE);
            if (read != TMP_TIME_IDX_SIZE) {
                throw new IllegalStateException("tmpTimeIdxFile文件损坏。");
            }
            fileInputStream.close();
            if (!tmpTimeIdxFile.delete()) {
                System.out.println(("tmpTimeIdxFile文件删除失败。"));
            }
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            batchItemCount = byteBuffer.getInt();
            minTime = byteBuffer.getLong();
            maxTime = byteBuffer.getLong();
            lastTime = byteBuffer.getLong();
        }
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
        maxTime = Math.max(maxTime, time);
        // todo 变长编码
        CommonUtils.writeInt(timeOutput, (int) (time - lastTime));
        lastTime = time;
        batchItemCount++;
        checkAndIndex();
    }

    public void shutdown() throws IOException {
        if (batchItemCount > 0) {
            if (!tmpTimeIdxFile.exists()) {
                tmpTimeIdxFile.createNewFile();
            }
            FileOutputStream fileOutputStream = new FileOutputStream(tmpTimeIdxFile, false);
            ByteBuffer byteBuffer = ByteBuffer.allocate(TMP_TIME_IDX_SIZE);
            byteBuffer.putInt(batchItemCount);
            byteBuffer.putLong(minTime);
            byteBuffer.putLong(maxTime);
            byteBuffer.putLong(lastTime);
            fileOutputStream.write(byteBuffer.array());
            fileOutputStream.flush();
            fileOutputStream.close();
        }

        timeOutput.flush();
        timeOutput.close();
        timeIndexOutput.flush();

//        System.out.println("shutdown " + vinStr + " 主键索引大小" + indexFileSize + "B");

        timeIndexOutput.close();
        timeInput.close();
        timeIndexInput.close();
    }

    private void index() throws IOException {
        // 输出主键稀疏索引 todo maxTime_delta, delta_bf
        TimeIndexItem timeIndexItem = new TimeIndexItem(minTime, maxTime);
        CommonUtils.writeLong(timeIndexOutput, timeIndexItem.getMinTime());
        CommonUtils.writeLong(timeIndexOutput, timeIndexItem.getMaxTime());
        timeIndexItemList.add(timeIndexItem);
        indexFileSize += TimeIndexItem.SIZE;
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

    private void loadAllIndex() throws IOException {
        timeIndexItemList.clear();
        MappedByteBuffer byteBuffer = timeIndexInput.map(FileChannel.MapMode.READ_ONLY, 0, indexFileSize);
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
            timeIndexItemList.add(new TimeIndexItem(minTime, maxTime));
        }
    }

    public List<TimeItem> range(long l, long r) throws IOException {
        timeOutput.flush();
        timeIndexOutput.flush();

        List<TimeItem> timeItemList = new ArrayList<>();

        int indexItemCount = timeIndexItemList.size();
        for (int i = 0; i < indexItemCount; i++) {
            long minTime = timeIndexItemList.get(i).getMinTime();
            long maxTime = timeIndexItemList.get(i).getMaxTime();
            if (l > maxTime || r <= minTime) {
                continue;
            }
            // 需要扫描这一批次
            timeItemList.addAll(range(l, r, i));
        }
        if (batchItemCount > 0) {
            if (l > maxTime || r <= minTime) {
                // no need
            } else {
                // 需要扫描这一批次
                timeItemList.addAll(range(l, r, indexItemCount));
            }
        }
        return timeItemList;
    }

    private List<TimeItem> range(long l, long r, int batchNum) throws IOException {
        List<TimeItem> timeItemList = new ArrayList<>();

        ByteBuffer byteBuffer = ByteBuffer.allocate(FULL_BATCH_SIZE);
        timeInput.read(byteBuffer, (long) batchNum * FULL_BATCH_SIZE);
        byteBuffer.flip();
        int pos = 0;
        long last = byteBuffer.getLong();
        if (last >= l && last < r) {
            timeItemList.add(new TimeItem(last, (long) batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos));
        }
        pos++;
        while (byteBuffer.remaining() > 0) {
            last = last + byteBuffer.getInt();
            if (last >= l && last < r) {
                timeItemList.add(new TimeItem(last, (long) batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos));
            }
            pos++;
        }
        return timeItemList;
    }

    public long getIndexFileSize() {
        return indexFileSize;
    }
}
