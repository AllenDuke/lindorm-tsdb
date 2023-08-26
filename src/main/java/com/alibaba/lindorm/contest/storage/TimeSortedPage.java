package com.alibaba.lindorm.contest.storage;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class TimeSortedPage extends AbPage {

    public TimeSortedPage(VinStorage vinStorage, BufferPool bufferPool, int num) {
        super(vinStorage, bufferPool, num);
    }

    /**
     * 左节点
     */
    private int leftNum;

    /**
     * 当前页最小key
     */
    private long minTime;

    /**
     * 右节点
     */
    private int rightNum;

    /**
     * 当前页最大key
     */
    private long maxTime;

    private final TreeMap<Long, Row> rowMap = new TreeMap<>();

    private List<ExtPage> extPageList;

    private int extNum = -1;

    @Override
    public synchronized void recover() throws IOException {
        if (stat != PageStat.FLUSHED) {
            return;
        }

        super.recover();

        leftNum = dataBuffer.unwrap().getInt();
        minTime = dataBuffer.unwrap().getLong();
        rightNum = dataBuffer.unwrap().getInt();
        maxTime = dataBuffer.unwrap().getLong();
        extNum = dataBuffer.unwrap().getInt();

        // 实际上也不会有循环recover
        int nextExtNum = extNum;
        while (nextExtNum >= 0 && nextExtNum != num) {
            // 有扩展页
            ExtPage extPage = vinStorage.getPage(ExtPage.class, nextExtNum);
            extPage.recover();
            extPageList.add(extPage);

            nextExtNum = extPage.nextExt();
        }

        stat = PageStat.RECOVERED_HEAD;
    }


    private void flushLarge() {
        // 没办法，需要进行一次额外的内存拷贝
        Row bigRow = rowMap.firstEntry().getValue();
        int rowSize = rowSize(bigRow);
        ByteBuffer allocate = ByteBuffer.allocate(rowSize);


        for (ExtPage extPage : extPageList) {

        }
    }

    @Override
    public void flush() throws IOException {
        dataBuffer.unwrap().position(0);
        dataBuffer.unwrap().putInt(leftNum);
        dataBuffer.unwrap().putLong(minTime);
        dataBuffer.unwrap().putInt(rightNum);
        dataBuffer.unwrap().putLong(maxTime);

        if (extPageList == null || extPageList.isEmpty()) {
            dataBuffer.unwrap().putInt(-1);
        } else {
            dataBuffer.unwrap().putInt(extPageList.get(0).num);
            flushLarge();
        }

        for (Row row : rowMap.values()) {
            dataBuffer.unwrap().putInt(rowSize(row));
            List<String> columnNameList = vinStorage.schema();
            for (String columnName : columnNameList) {
                ColumnValue cVal = row.getColumns().get(columnName);
                switch (cVal.getColumnType()) {
                    case COLUMN_TYPE_STRING:
                        dataBuffer.unwrap().putInt(cVal.getStringValue().limit());
                        dataBuffer.unwrap().put(cVal.getStringValue());
                        break;
                    case COLUMN_TYPE_INTEGER:
                        dataBuffer.unwrap().putInt(cVal.getIntegerValue());
                        break;
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        dataBuffer.unwrap().putDouble(cVal.getDoubleFloatValue());
                        break;
                    default:
                        throw new IllegalStateException("Invalid column type");
                }
            }
        }

        super.flush();

        // 释放内存
        rowMap.clear();
        extPageList.clear();
    }

    private void first(long k, Row v) {
        minTime = k;
        maxTime = k;
        leftNum = -1;
        rightNum = -1;
    }

    /**
     * 插入大数据
     *
     * @param k
     * @param v
     */
    private void insertLarge(long k, Row v, int vTotalSize) {
        vTotalSize = vTotalSize + 4 - dataBuffer.unwrap().remaining();
        dataBuffer.unwrap().position(dataBuffer.unwrap().capacity());

        extPageList = new LinkedList<>();
        while (vTotalSize > 0) {
            ExtPage extPage = vinStorage.creatPage(ExtPage.class);
            vTotalSize -= extPage.dataCapacity();
            extPageList.add(extPage);
        }

        rowMap.put(k, v);
    }

    /**
     * k v会立即开始读
     *
     * @param k
     * @param v
     * @return 如果可插入当前节点，那么返回当前页号，否则返回下一个尝试插入的页号
     */
    public int insert(long k, Row v) throws IOException {
        recover();
        if (!rowMap.isEmpty() && (k < minTime || k > maxTime)) {
            // 不能插入当前节点
            if (k < minTime) {
                return leftNum;
            } else {
                return rightNum;
            }
        }

        // 准备插入当前节点
        synchronized (this) {
            if (rowMap.isEmpty()) {
                first(k, v);
            }

            // 插入map
            rowMap.put(k, v);

            // 4字节记录行数据大小，接着记录行数据
            int vTotalSize = rowSize(v);
            int position = dataBuffer.unwrap().position() + 4 + vTotalSize;

            /**
             * 检查容量
             * 如果当前节点不足以完整插入该行记录，那么当前节点分裂，从当前页的尾部节点开始拷贝节点到新的一页
             */
            List<Row> transfer = null;
            Map.Entry<Long, Row> lastEntry = null;
            while (position > dataBuffer.unwrap().capacity()) {
                if (transfer == null) {
                    transfer = new LinkedList<>();
                }
                lastEntry = rowMap.pollLastEntry();
                transfer.add(lastEntry.getValue());
                position -= 4 + rowSize(lastEntry.getValue());
            }
            if (transfer != null && !transfer.isEmpty()) {
                // 转移到新的一页
                TimeSortedPage newPage = vinStorage.creatPage(TimeSortedPage.class);
                for (Row row : transfer) {
                    newPage.insert(row.getTimestamp(), row);
                }
                // 调整链表
                this.connect(newPage);
            }
            if (rowMap.isEmpty() && lastEntry != null) {
                // 当前的第一个节点即为大节点
                insertLarge(k, v, vTotalSize);
                return num;
            }

            dataBuffer.unwrap().position(position);
        }

        return num;
    }

    private void checkConnect(TimeSortedPage page) {
        if (page.minTime >= this.minTime && page.minTime <= this.maxTime) {
            throw new IllegalStateException("page与当前右相交");
        }
        if (page.maxTime >= this.minTime && page.maxTime <= this.maxTime) {
            throw new IllegalStateException("page与当前左相交");
        }
    }

    /**
     * 连接另一页
     *
     * @param page
     */
    public synchronized void connect(TimeSortedPage page) {
        checkConnect(page);
        if (page.minTime > this.maxTime) {
            // page为this的右节点
            TimeSortedPage oldRPage = vinStorage.getPage(TimeSortedPage.class, this.rightNum);
            if (oldRPage != null) {
                oldRPage.leftNum = page.num;
                page.rightNum = oldRPage.num;
            }
            this.rightNum = page.num;
        }
        if (page.maxTime < this.minTime) {
            // page为this的左节点
            TimeSortedPage oldLPage = vinStorage.getPage(TimeSortedPage.class, this.leftNum);
            if (oldLPage != null) {
                oldLPage.rightNum = page.num;
                page.leftNum = page.num;
            }
            this.leftNum = page.num;
        }
    }

    /**
     * 计算一行数据序列化后大小，这里不用ByteBuffer，是为了避免频繁地在内存中拷贝。
     * todo 缓存大小，避免多次计算
     *
     * @param row
     * @return
     */
    private int rowSize(Row row) {
        // todo 暂不计算vin
        int size = 0;

        // 时间戳
        size += 8;

        List<String> columnNameList = vinStorage.schema();
        for (String columnName : columnNameList) {
            ColumnValue columnValue = row.getColumns().get(columnName);
            switch (columnValue.getColumnType()) {
                case COLUMN_TYPE_STRING:
                    size += 4;
                    size += columnValue.getStringValue().remaining();
                    break;
                case COLUMN_TYPE_INTEGER:
                    size += 4;
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    size += 8;
                    break;
                default:
                    throw new IllegalStateException("Invalid column type");
            }
        }

        return size;
    }
}
