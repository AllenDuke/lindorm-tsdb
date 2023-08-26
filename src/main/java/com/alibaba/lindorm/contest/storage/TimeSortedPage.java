package com.alibaba.lindorm.contest.storage;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;

import java.io.IOException;
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

    private TreeMap<Long, Row> map = new TreeMap<>();

    private List<ExtPage> extPageList;

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

        // 实际上也不会有循环recover
        int nextExtNum = dataBuffer.unwrap().getInt();
        while (nextExtNum >= 0 && nextExtNum != num) {
            // 有扩展页
            ExtPage extPage = vinStorage.getPage(nextExtNum);
            extPage.recover();
            extPageList.add(extPage);

            nextExtNum = extPage.nextExt();
        }

        stat = PageStat.USING;
    }

    @Override
    public void flush() throws IOException {
        for (Row row : map.values()) {
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
        map.clear();
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

        map.put(k, v);
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
        if (!map.isEmpty() && (k < minTime || k > maxTime)) {
            // 不能插入当前节点
            if (k < minTime) {
                return leftNum;
            } else {
                return rightNum;
            }
        }

        // 准备插入当前节点
        synchronized (this) {
            if (map.isEmpty()) {
                first(k, v);
            }

            // 插入map
            map.put(k, v);

            // 4字节记录行数据大小，接着记录行数据
            int vTotalSize = rowSize(v);

            /**
             * 检查容量
             * 如果当前节点不足以完整插入该行记录，当前节点分裂，从当前页的尾部节点开始拷贝节点到新的一页
             */
            Map.Entry<Long, Row> lastEntry = map.lastEntry();
            List<Row> transfer = new LinkedList<>();
            int position = dataBuffer.unwrap().position() + 4 + vTotalSize;
            while (position > dataBuffer.unwrap().capacity()) {
                transfer.add(lastEntry.getValue());
                position -= 4 + rowSize(lastEntry.getValue());
            }

            if (dataBuffer.unwrap().remaining() < 4 + vTotalSize) {
                SortedMap<Long, Row> tailMap = map.tailMap(k);
                if (!tailMap.isEmpty()) {

                }

                if (map.isEmpty()) {
                    // 当前是该页第一行数据，而且是大
                    insertLarge(k, v, vTotalSize);
                    return num;
                }

                //
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

            TimeSortedPage oldRPage = vinStorage.getPage(this.rightNum);
            if (oldRPage != null) {
                oldRPage.leftNum = page.num;
                page.rightNum = oldRPage.num;
            }
            this.rightNum = page.num;
        }
        if (page.maxTime < this.minTime) {
            // page为this的左节点

            TimeSortedPage oldLPage = vinStorage.getPage(this.leftNum);
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
