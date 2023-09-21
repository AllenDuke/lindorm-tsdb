package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.CommonUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TimeChannel {

    private static final int FULL_BATCH_SIZE = (LsmStorage.MAX_ITEM_CNT_L0 - 1) * 4 + 8;

    private static final int TMP_TIME_IDX_SIZE = 4 + 8 + 8 + 8;

    private final DataChannel timeOutput;

    private final File timeFile;

    private final OutputStream timeIndexOutput;

    private final File timeIdxFile;

    private final File tmpTimeIdxFile;

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

    private RandomAccessFile timeInput;

    public TimeChannel(File vinDir) throws IOException {
        timeFile = new File(vinDir.getAbsolutePath(), "time");
        if (!timeFile.exists()) {
            timeFile.createNewFile();
        }

        timeOutput = new DataChannel(timeFile, LsmStorage.IO_MODE, 8, LsmStorage.OUTPUT_BUFFER_SIZE);
        timeInput = new RandomAccessFile(timeFile, "r");

        vinStr = vinDir.getName();

        timeIdxFile = new File(vinDir.getAbsolutePath(), "time.idx");
        if (!timeIdxFile.exists()) {
            timeIdxFile.createNewFile();
        }
        timeIndexOutput = new BufferedOutputStream(new FileOutputStream(timeIdxFile, true), LsmStorage.OUTPUT_BUFFER_SIZE);
        indexFileSize = timeIdxFile.length();

        loadAllIndexForInit();

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

            timeOutput.writeLong(time);

            batchItemCount++;
            return;
        }
        minTime = Math.min(minTime, time);
        maxTime = Math.max(maxTime, time);

        // todo 变长编码
        timeOutput.writeInt((int) (time - lastTime));

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

//        System.out.println("shutdown " + vinStr + " 主键索引大小" + indexFileSize + "B");

        timeIndexOutput.flush();
        timeIndexOutput.close();

        timeInput.close();
    }

    public void flush() throws IOException {
        timeOutput.flush();
        timeIndexOutput.flush();
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

    private void loadAllIndexForInit() throws IOException {
        timeIndexItemList.clear();
        FileInputStream fileInputStream = new FileInputStream(timeIdxFile);
        ByteBuffer byteBuffer = ByteBuffer.allocate((int) timeIdxFile.length());
        if (fileInputStream.read(byteBuffer.array()) != (int) timeIdxFile.length()) {
            throw new IllegalStateException("主键稀疏索引文件损坏");
        }
        fileInputStream.close();

        if (byteBuffer.limit() % 4 != 0) {
            throw new IllegalStateException("主键稀疏索引文件损坏");
        }
        int indexItemCount = byteBuffer.limit() / TimeIndexItem.SIZE;

        for (int i = 0; i < indexItemCount; i++) {
            long minTime = byteBuffer.getLong();
            long maxTime = byteBuffer.getLong();
            timeIndexItemList.add(new TimeIndexItem(minTime, maxTime));
        }
    }

    public List<TimeItem> agg(long l, long r) throws IOException {
        boolean flushed = false;

        List<TimeItem> timeItemList = new ArrayList<>();

        int indexItemCount = timeIndexItemList.size();
        for (int i = 0; i < indexItemCount; i++) {
            long minTime = timeIndexItemList.get(i).getMinTime();
            long maxTime = timeIndexItemList.get(i).getMaxTime();
            if (l > maxTime || r <= minTime) {
                continue;
            }
            // 需要扫描这一批次
            if (minTime >= l && maxTime < r) {
                // 这一完整批次符合条件，但因为是agg，所以不需要扫描数据文件，在内存中生成
                for (int itemNum = 0; itemNum < LsmStorage.MAX_ITEM_CNT_L0; itemNum++) {
                    timeItemList.add(new TimeItem(0, (long) LsmStorage.MAX_ITEM_CNT_L0 * i + itemNum));
                }
            } else {
                if (!flushed) {
                    flush();
                    flushed = true;
                }
                timeItemList.addAll(range(l, r, i, timeInput));
            }
        }
        if (batchItemCount > 0) {
            if (l > maxTime || r <= minTime) {
                // no need
            } else {
                // 需要扫描这一批次
                if (minTime >= l && maxTime < r) {
                    // 这一完整批次符合条件，但因为是agg，所以不需要扫描数据文件，在内存中生成
                    for (int itemNum = 0; itemNum < batchItemCount; itemNum++) {
                        timeItemList.add(new TimeItem(0, (long) LsmStorage.MAX_ITEM_CNT_L0 * indexItemCount + itemNum));
                    }
                } else {
                    if (!flushed) {
                        flush();
                        flushed = true;
                    }
                    timeItemList.addAll(range(l, r, indexItemCount, timeInput));
                }
            }
        }

        return timeItemList;
    }

    /**
     * todo 读写锁
     *
     * @param l
     * @param r
     * @return
     * @throws IOException
     */
    public List<TimeItem> range(long l, long r) throws IOException {
        flush();

        List<TimeItem> timeItemList = new ArrayList<>();

        int indexItemCount = timeIndexItemList.size();
        for (int i = 0; i < indexItemCount; i++) {
            long minTime = timeIndexItemList.get(i).getMinTime();
            long maxTime = timeIndexItemList.get(i).getMaxTime();
            if (l > maxTime || r <= minTime) {
                continue;
            }
            // 需要扫描这一批次
            timeItemList.addAll(range(l, r, i, timeInput));
        }
        if (batchItemCount > 0) {
            if (l > maxTime || r <= minTime) {
                // no need
            } else {
                // 需要扫描这一批次
                timeItemList.addAll(range(l, r, indexItemCount, timeInput));
            }
        }

        return timeItemList;
    }

    private List<TimeItem> range(long l, long r, int batchNum, RandomAccessFile timeInput) throws IOException {
        List<TimeItem> timeItemList = new ArrayList<>();

//        ByteBuffer byteBuffer = ByteBuffer.allocate(FULL_BATCH_SIZE);
//        timeInput.seek((long) batchNum * FULL_BATCH_SIZE);
//        int read = timeInput.read(byteBuffer.array());
//        byteBuffer.limit(read);

        ByteBuffer byteBuffer = timeOutput.read((long) batchNum * FULL_BATCH_SIZE, FULL_BATCH_SIZE);

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
