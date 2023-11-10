package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.util.ByteBufferUtil;
import com.alibaba.lindorm.contest.util.NumberUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class TimeChannel {

    public static final AtomicLong ORIG_SIZE = new AtomicLong(0);
    public static final AtomicLong REAL_SIZE = new AtomicLong(0);

    /**
     * 所有vin共享一个大缓存池。
     * BYTE_BUFFER_MAP，k高32位为vin.hashcode
     */
    private static final AtomicLong AVAILABLE = new AtomicLong(256 * 1024 * 1024);
    private static final Map<Long, List<Integer>> INTS_MAP = Collections.synchronizedMap(new LinkedHashMap<>());

    private final DataChannel timeOutput;

    private long indexFileSize;

    private long minTime;

    private long maxTime;

    private long lastTime;

    private int batchItemCount;

    /**
     * 主键稀疏索引常驻内存
     */
    private final List<TimeIndexItem> timeIndexItemList;

    private boolean isDirty;

    private long batchPos;
    private int batchSize;

    private final int vinHashCode;

    private Map<Integer, ByteBuffer> byteBufferMap = new HashMap<>();

    private Map<Integer, Long> batchFirstMap = new HashMap<>();

    public TimeChannel(int vinHashCode, DataChannel dataChannel, List<TimeIndexItem> timeIndexItemList) throws IOException {
        this.vinHashCode = vinHashCode;
        timeOutput = dataChannel;
        this.timeIndexItemList = timeIndexItemList;
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
        ByteBuffer byteBuffer = NumberUtil.zIntDeltaOfDelta(ints);
        byte[] bytes = ByteBufferUtil.zstdEncode(byteBuffer);
        timeOutput.writeBytes(bytes);

        batchItemCount = times.length;
        batchSize = 8 + bytes.length;
        REAL_SIZE.addAndGet(batchSize);
    }

    public void shutdown() throws IOException {
        timeOutput.flush();
        timeOutput.close();
    }

    public void flush() throws IOException {
        if (!isDirty) {
            return;
        }
        timeOutput.flush();

        isDirty = false;
    }

    public TimeIndexItem index() throws IOException {
        // 输出主键稀疏索引
        TimeIndexItem timeIndexItem = new TimeIndexItem(minTime, maxTime, batchPos, batchSize);
//        CommonUtils.writeLong(timeIndexOutput, timeIndexItem.getMinTime());
//        CommonUtils.writeLong(timeIndexOutput, timeIndexItem.getMaxTime());
//        CommonUtils.writeLong(timeIndexOutput, timeIndexItem.getPos());
//        CommonUtils.writeInt(timeIndexOutput, timeIndexItem.getSize());
        timeIndexItemList.add(timeIndexItem);
        indexFileSize += TimeIndexItem.SIZE;

        batchItemCount = 0;

        return timeIndexItem;
    }

    public TimeIndexItem readIndexItem(ByteBuffer byteBuffer) {
        long minTime = byteBuffer.getLong();
        long maxTime = byteBuffer.getLong();
        long pos = byteBuffer.getLong();
        int size = byteBuffer.getInt();
        return new TimeIndexItem(minTime, maxTime, pos, size);
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
                timeItemList.addAll(range(l, r, i));
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
                    timeItemList.addAll(range(l, r, indexItemCount));
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

//        ByteBuffer byteBuffer = ByteBuffer.allocate(FULL_BATCH_SIZE);
//        timeInput.seek((long) batchNum * FULL_BATCH_SIZE);
//        int read = timeInput.read(byteBuffer.array());
//        byteBuffer.limit(read);

        long k = intsMapK(batchNum);
        List<Integer> ints = INTS_MAP.get(k);

        int pos = 0;
        Long last = batchFirstMap.get(batchNum);
        if (ints == null) {
            ByteBuffer byteBuffer = byteBufferMap.get(batchNum);
            if (byteBuffer != null) {
                byteBuffer.clear();
                last = batchFirstMap.get(batchNum);
            } else {
                TimeIndexItem timeIndexItem = timeIndexItemList.get(batchNum);
                byteBuffer = timeOutput.read(timeIndexItem.getPos(), timeIndexItem.getSize());
                last = byteBuffer.getLong();
                batchFirstMap.put(batchNum, last);

                byteBuffer = ByteBuffer.wrap(ByteBufferUtil.zstdDecode(byteBuffer));
                byteBufferMap.put(batchNum, byteBuffer);
            }

            ints = NumberUtil.rzIntDeltaOfDelta(byteBuffer);
            cache(k, ints);
        }
        if (last >= l && last < r) {
            timeItemList.add(new TimeItem(last, (long) batchNum * LsmStorage.MAX_ITEM_CNT_L0 + pos));
        }
        pos++;
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

    private long intsMapK(int batchNum) {
        // todo 换成vinId
        long k = vinHashCode;
        k = k << 32;
        k = k | batchNum;
        return k;
    }

    private void cache(long k, List<Integer> ints) {
        AVAILABLE.addAndGet(-((long) ints.size() << 2));
        synchronized (INTS_MAP) {
            INTS_MAP.put(k, ints);
            // 避免并发修改
            Iterator<Map.Entry<Long, List<Integer>>> entryIterator = INTS_MAP.entrySet().iterator();
            while (entryIterator.hasNext()) {
                if (AVAILABLE.get() > 0) {
                    break;
                }
                List<Integer> remove = entryIterator.next().getValue();
                AVAILABLE.addAndGet((long) remove.size() << 2);
                entryIterator.remove();
            }
        }
    }

}
