package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.util.ByteBufferUtil;
import com.alibaba.lindorm.contest.util.NumberUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class TimeChannel {

    public static final AtomicLong ORIG_SIZE = new AtomicLong(0);
    public static final AtomicLong REAL_SIZE = new AtomicLong(0);

    private final DataChannel timeOutput;

    private final File timeFile;

    private final OutputStream timeIndexOutput;

    private final File timeIdxFile;

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

    private boolean isDirty;

    private int loadedAllIndexForInit = -1;

    private long batchPos;
    private int batchSize;

    private Map<Integer, ByteBuffer> byteBufferMap = new HashMap<>();

    public TimeChannel(File vinDir) throws IOException {
        timeFile = new File(vinDir.getAbsolutePath(), "time.data");
        if (!timeFile.exists()) {
            timeFile.createNewFile();
        }

        timeOutput = new DataChannel(timeFile, LsmStorage.IO_MODE, 16, LsmStorage.OUTPUT_BUFFER_SIZE);
        timeInput = new RandomAccessFile(timeFile, "r");

        vinStr = vinDir.getName();

        timeIdxFile = new File(vinDir.getAbsolutePath(), "time.idx");
        if (!timeIdxFile.exists()) {
            timeIdxFile.createNewFile();
        }
        timeIndexOutput = new BufferedOutputStream(new FileOutputStream(timeIdxFile, true), LsmStorage.OUTPUT_BUFFER_SIZE);
        indexFileSize = timeIdxFile.length();

        loadAllIndexForInit();
    }

    public void append(long[] times) throws IOException {
        ORIG_SIZE.addAndGet(8L * times.length);
        isDirty = true;

        batchPos = timeOutput.channelSize();

        REAL_SIZE.addAndGet(8);
        minTime = times[0];
        maxTime = times[0];
        lastTime = times[0];
        // 919721
        int[] ints = new int[times.length - 1];
        for (int i = 1; i < times.length; i++) {
            long time = times[i];
            minTime = Math.min(minTime, time);
            maxTime = Math.max(maxTime, time);
            ints[i - 1] = (int) (time - lastTime);
            lastTime = time;
        }
        timeOutput.writeLong(times[0]);
        ByteBuffer byteBuffer = NumberUtil.zInt(ints);
        byte[] bytes = ByteBufferUtil.zstdEncode(byteBuffer);
        timeOutput.writeBytes(bytes);

        batchItemCount = times.length;
        batchSize = 8 + bytes.length;
        REAL_SIZE.addAndGet(batchSize);
    }

    public void shutdown() throws IOException {
        timeOutput.flush();
        timeOutput.close();

        timeIndexOutput.flush();
        timeIndexOutput.close();

        timeInput.close();
    }

    public void flush() throws IOException {
        if (!isDirty) {
            return;
        }
        timeOutput.flush();
        timeIndexOutput.flush();
        isDirty = false;
    }

    public void index() throws IOException {
        // 输出主键稀疏索引 todo maxTime_delta, delta_bf
        TimeIndexItem timeIndexItem = new TimeIndexItem(minTime, maxTime, batchPos, batchSize);
        CommonUtils.writeLong(timeIndexOutput, timeIndexItem.getMinTime());
        CommonUtils.writeLong(timeIndexOutput, timeIndexItem.getMaxTime());
        CommonUtils.writeLong(timeIndexOutput, timeIndexItem.getPos());
        CommonUtils.writeInt(timeIndexOutput, timeIndexItem.getSize());
        timeIndexItemList.add(timeIndexItem);
        indexFileSize += TimeIndexItem.SIZE;

        batchItemCount = 0;
    }

    public List<TimeIndexItem> loadAllIndexForInit() throws IOException {
        if (loadedAllIndexForInit != -1) {
            return timeIndexItemList;
        }
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
            long pos = byteBuffer.getLong();
            int size = byteBuffer.getInt();
            timeIndexItemList.add(new TimeIndexItem(minTime, maxTime, pos, size));
        }
        loadedAllIndexForInit = timeIndexItemList.size();
        return timeIndexItemList;
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
                timeItemList.add(new TimeItem(0, (long) LsmStorage.MAX_ITEM_CNT_L0 * i));
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
                    timeItemList.add(new TimeItem(0, (long) LsmStorage.MAX_ITEM_CNT_L0 * indexItemCount));
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

        ByteBuffer byteBuffer = byteBufferMap.get(batchNum);
        if (byteBuffer != null) {
            byteBuffer.clear();
        } else {
            TimeIndexItem timeIndexItem = timeIndexItemList.get(batchNum);
            byteBuffer = timeOutput.read(timeIndexItem.getPos(), timeIndexItem.getSize());
            byteBufferMap.put(batchNum, byteBuffer);
        }

        int pos = 0;
        long last = byteBuffer.getLong();
        if (last >= l && last < r) {
            timeItemList.add(new TimeItem(last, (long) batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos));
        }
        pos++;
        byteBuffer = ByteBuffer.wrap(ByteBufferUtil.zstdDecode(byteBuffer));
        List<Integer> ints = NumberUtil.rzInt(byteBuffer);
        for (Integer delta : ints) {
            last = last + delta;
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

    public int batchIndexCount() {
        return timeIndexItemList.size();
    }
}
