package com.alibaba.lindorm.contest.storage;

import com.alibaba.lindorm.contest.mem.MemPagePool;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class TimeSortedPage extends AbPage {

    private static final int HEAD_SIZE = 4 + 8 + 4 + 8 + 4 + 4;

    public TimeSortedPage(VinStorage vinStorage, Integer num) {
        super(vinStorage, num);
        leftNum = -1;
        rightNum = -1;
        minTime = -1;
        maxTime = -1;
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

    /**
     * 占位，recover时需要
     * 如果extNum==-1，那么表示当前页行数量。
     * 如果extNum!=-1，那么表示当前大行的大小
     */
    private int rowCountOrBigRowSize;

    @Override
    public void recover() throws IOException {
        super.recover();

        // 先恢复头数据
        recoverHead();
    }

    private synchronized void recoverHead() throws IOException {
        leftNum = memPage.unwrap().getInt();
        minTime = memPage.unwrap().getLong();
        rightNum = memPage.unwrap().getInt();
        maxTime = memPage.unwrap().getLong();
        extNum = memPage.unwrap().getInt();
        if (extNum != -1) {
            extPageList = new ArrayList<>();
        }
        rowCountOrBigRowSize = memPage.unwrap().getInt();
    }


    private synchronized void recoverAll() throws IOException {
        if (memPage.unwrap().position() != HEAD_SIZE || !rowMap.isEmpty()) {
            return;
        }

        // 实际上也不会有循环recover
        int nextExtNum = extNum;
        while (nextExtNum >= 0) {
            // 有扩展页
            ExtPage extPage = vinStorage.getPage(ExtPage.class, nextExtNum);
            extPage.recover();
            extPageList.add(extPage);

            nextExtNum = extPage.nextExt();
        }

        // 恢复rowMap todo 插入时只需要key信息，不需要recover整行
        if (extNum == -1) {
            recoverNormal();
        } else {
            recoverLarge();
        }
    }

    private void recoverLarge() {
        /**
         * 大->小，rowCountOrBigRowSize使得数据准确。
         * 但当前扩展页只增不减，可能造成浪费
         */

        ByteBuffer allocate = ByteBuffer.allocate(rowCountOrBigRowSize);
        allocate.put(memPage.unwrap());
        for (ExtPage extPage : extPageList) {
            ByteBuffer memPage = extPage.getData();
            allocate.put(memPage);
        }
        allocate.flip();

        long timestamp = allocate.getLong();

        ArrayList<ColumnValue.ColumnType> columnTypeList = vinStorage.columnTypeList();
        ArrayList<String> columnNameList = vinStorage.columnNameList();
        Map<String, ColumnValue> columns = new HashMap<>();
        for (int i = 0; i < columnTypeList.size(); i++) {
            ColumnValue.ColumnType columnType = columnTypeList.get(i);
            String columnName = columnNameList.get(i);
            ColumnValue cVal;
            switch (columnType) {
                case COLUMN_TYPE_INTEGER:
                    int intVal = allocate.getInt();
                    cVal = new ColumnValue.IntegerColumn(intVal);
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    double doubleVal = allocate.getDouble();
                    cVal = new ColumnValue.DoubleFloatColumn(doubleVal);
                    break;
                case COLUMN_TYPE_STRING:
                    int strLen = allocate.getInt();
                    byte[] strBytes = new byte[strLen];
                    ByteBuffer strBuffer = allocate.get(strBytes);
                    strBuffer.flip();
                    cVal = new ColumnValue.StringColumn(strBuffer);
                    break;
                default:
                    throw new IllegalStateException("Undefined column type, this is not expected");
            }
            columns.put(columnName, cVal);
        }
        Row bigRow = new Row(vinStorage.vin(), timestamp, columns);
        rowMap.put(timestamp, bigRow);
    }

    private void flushLarge() throws IOException {
        // 没办法，需要进行一次额外的内存拷贝
        Row bigRow = rowMap.firstEntry().getValue();
        int bigRowSize = rowSize(bigRow);
        memPage.unwrap().putInt(bigRowSize);

        ByteBuffer allocate = ByteBuffer.allocate(bigRowSize);

        allocate.putLong(bigRow.getTimestamp());
        List<String> columnNameList = vinStorage.columnNameList();
        for (String columnName : columnNameList) {
            ColumnValue cVal = bigRow.getColumns().get(columnName);
            switch (cVal.getColumnType()) {
                case COLUMN_TYPE_STRING:
                    allocate.putInt(cVal.getStringValue().limit());
                    allocate.put(cVal.getStringValue());
                    break;
                case COLUMN_TYPE_INTEGER:
                    allocate.putInt(cVal.getIntegerValue());
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    allocate.putDouble(cVal.getDoubleFloatValue());
                    break;
                default:
                    throw new IllegalStateException("Invalid column type");
            }
        }
        allocate.flip();

        int remaining = memPage.unwrap().remaining();
        memPage.unwrap().put(allocate.slice().limit(remaining));
        allocate.position(remaining);
        for (ExtPage extPage : extPageList) {
            int dataCapacity = extPage.dataCapacity();
            ByteBuffer memPage = allocate.slice();
            memPage.limit(Math.min(dataCapacity, allocate.remaining()));
            extPage.putData(memPage);
            extPage.flush();

            allocate.position(allocate.position() + memPage.limit());
        }
    }

    private void flushNormal() {
        memPage.unwrap().putInt(rowMap.size());

        for (Row row : rowMap.values()) {
            memPage.unwrap().putLong(row.getTimestamp());
            List<String> columnNameList = vinStorage.columnNameList();
            for (String columnName : columnNameList) {
                ColumnValue cVal = row.getColumns().get(columnName);
                switch (cVal.getColumnType()) {
                    case COLUMN_TYPE_STRING:
                        memPage.unwrap().putInt(cVal.getStringValue().limit());
                        memPage.unwrap().put(cVal.getStringValue());
                        break;
                    case COLUMN_TYPE_INTEGER:
                        memPage.unwrap().putInt(cVal.getIntegerValue());
                        break;
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        memPage.unwrap().putDouble(cVal.getDoubleFloatValue());
                        break;
                    default:
                        throw new IllegalStateException("Invalid column type");
                }
            }
        }
    }

    private void recoverNormal() {
        ArrayList<ColumnValue.ColumnType> columnTypeList = vinStorage.columnTypeList();
        ArrayList<String> columnNameList = vinStorage.columnNameList();

        int rowCount = rowCountOrBigRowSize;
        for (int i = 0; i < rowCount; i++) {
            long timestamp = memPage.unwrap().getLong();
            Map<String, ColumnValue> columns = new HashMap<>();
            for (int j = 0; j < columnTypeList.size(); j++) {
                ColumnValue.ColumnType columnType = columnTypeList.get(j);
                String columnName = columnNameList.get(j);
                ColumnValue cVal;
                switch (columnType) {
                    case COLUMN_TYPE_INTEGER:
                        int intVal = memPage.unwrap().getInt();
                        cVal = new ColumnValue.IntegerColumn(intVal);
                        break;
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        double doubleVal = memPage.unwrap().getDouble();
                        cVal = new ColumnValue.DoubleFloatColumn(doubleVal);
                        break;
                    case COLUMN_TYPE_STRING:
                        int strLen = memPage.unwrap().getInt();
                        byte[] strBytes = new byte[strLen];
                        memPage.unwrap().get(strBytes);
                        cVal = new ColumnValue.StringColumn(ByteBuffer.wrap(strBytes));
                        break;
                    default:
                        throw new IllegalStateException("Undefined column type, this is not expected");
                }
                columns.put(columnName, cVal);
            }
            Row bigRow = new Row(vinStorage.vin(), timestamp, columns);
            rowMap.put(timestamp, bigRow);
        }
    }

    @Override
    public void flush() throws IOException {
        if (rowMap.isEmpty()) {
            // 无脏数据，不需要刷盘
            return;
        }

        recoverAll();

        if (rowMap.isEmpty()) {
            throw new IllegalStateException("刷盘异常，页数据为空");
        }

        memPage.unwrap().position(0);
        memPage.unwrap().putInt(leftNum);
        memPage.unwrap().putLong(minTime);
        memPage.unwrap().putInt(rightNum);
        memPage.unwrap().putLong(maxTime);

        if (extNum == -1) {
            // 用extNum判断才是正确，因为当前页可能因为更新而大变小
            memPage.unwrap().putInt(-1);
            flushNormal();
        } else {
            memPage.unwrap().putInt(extPageList.get(0).num);
            flushLarge();
        }

        super.flush();

        // 释放内存
        rowMap.clear();
        extPageList = null;
    }

    private void firstInsert(long k, Row v) {
        minTime = k;
        maxTime = k;
//        leftNum = -1;
//        rightNum = -1;

        extNum = -1;
//        rowCountOrBigRowSize = rowSize(v);
        memPage.unwrap().position(HEAD_SIZE);
    }

    /**
     * 插入大数据
     *
     * @param k
     * @param v
     */
    private void insertLarge(long k, Row v, int newRowSize) throws IOException {
//        rowCountOrBigRowSize = vTotalSize;
        System.out.println("发现大节点" + newRowSize);

        // 4字节保存行大小
        int totalSize = newRowSize + 4;

        // 填满当前页
        totalSize -= memPage.unwrap().remaining();
        memPage.unwrap().position(memPage.unwrap().limit());

        if (extPageList == null) {
            // 不为null表示更新
            extPageList = new ArrayList<>();
        }
        for (int i = 0; i < extPageList.size(); i++) {
            ExtPage extPage = extPageList.get(i);
            totalSize -= extPage.dataCapacity();
        }
        if (totalSize <= 0) {
            // 新的数据更短 todo 归还扩展页
        } else {
            while (totalSize > 0) {
                ExtPage extPage = vinStorage.creatPage(ExtPage.class);
                totalSize -= extPage.dataCapacity();
                extPageList.add(extPage);
            }
            // 连接链表
            for (int i = 0; i < extPageList.size() - 1; i++) {
                extPageList.get(i).nextExt(extPageList.get(i + 1).num);
            }
            extPageList.get(extPageList.size() - 1).nextExt(-1);
        }

        extNum = extPageList.get(0).num;

        rowMap.put(k, v);
    }

    private void updateTimeWindowBeforeFlushing() throws IOException {
        minTime = rowMap.firstKey();
        maxTime = rowMap.lastKey();
    }

    protected WindowSearchResult search(WindowSearchRequest request) throws IOException {
        WindowSearchResult result = new WindowSearchResult(this.num);
        if (this.minTime == -1 || this.maxTime == -1) {
            result.setNextLeft(-1);
            result.setNextRight(-1);
            return result;
        }

        long leftTime = request.getMinTime();
        if (leftTime > this.maxTime) {
            // 往右边寻找
            result.setNextRight(this.rightNum);
            result.setNextLeft(-1);
            return result;
        }

        long rightTime = request.getMaxTime();
        if (rightTime < this.minTime) {
            // 往左边寻找
            result.setNextLeft(this.leftNum);
            result.setNextRight(-1);
            return result;
        }

        // 当前页存在数据
        recoverAll();

        // 左闭右开区间
        SortedMap<Long, Row> map = rowMap.tailMap(leftTime).headMap(rightTime);
        Collection<Row> rows = map.values();
        result.setRowList(new LinkedList<>(rows));

        if (leftTime < minTime) {
            // 需要继续往左寻找
            result.setNextLeft(this.leftNum);
        }
        if (rightTime > maxTime + 1) {
            // 需要继续往右寻找
            result.setNextRight(this.rightNum);
        }
        return result;
    }

    /**
     * k v会立即开始读
     *
     * @param k
     * @param v
     * @return 如果可插入当前节点，那么返回当前页号，否则返回下一个尝试插入的页号
     */
    protected int insert(long k, Row v) throws IOException {
        if (minTime != -1 && extNum != -1) {
            // 当前页存放的是大节点，不接受不相等的数据插入
            if (k < minTime) {
                return leftNum;
            } else if (k > maxTime) {
                return rightNum;
            }
        }

        // 这里不用rowMap是否empty来判断页是否为空，因为recoverHead不会去构建rowMap
        if (minTime != -1 && extNum == -1 && k < minTime) {
            // 不能插入当前节点
            return leftNum;
        }

        /**
         * 到达此处可能的case:
         * 1。当前存放的是大节点，发生更新
         * 2.当前存放的是小节点，k>=minTime
         * 3.当前页还没有数据
         */

        // 准备插入当前节点

        recoverAll();

        if (rowMap.isEmpty()) {
            firstInsert(k, v);
        }

        // 准备调整指针
        int position = memPage.unwrap().position();
        // 准备追加行数据
        int newRowSize = rowSize(v);
        position += newRowSize;

        // 面向特殊编程
        if (k > maxTime && position > memPage.unwrap().limit() && rightNum != -1) {
            TimeSortedPage nextTry = vinStorage.getPage(TimeSortedPage.class, rightNum);
            if (k >= nextTry.minTime) {
                return rightNum;
            }
        }

        // 插入map
        Row oldV = rowMap.put(k, v);

        if (oldV != null) {
            // 发生更新
            int oldRowSize = rowSize(oldV);
            if (extNum == -1) {
                position -= oldRowSize;
            } else {
                // todo 回收之前的扩展页
                firstInsert(k, v);
            }
        }

        /**
         * 检查容量
         * 如果当前节点不足以完整插入该行记录，那么当前节点分裂，从当前页的尾部节点开始拷贝节点到新的一页
         */
        List<Row> transfer = null;
        Map.Entry<Long, Row> lastEntry = null;
        while (position > memPage.unwrap().limit()) {
            if (transfer == null) {
                transfer = new LinkedList<>();
            }
            lastEntry = rowMap.pollLastEntry();
            transfer.add(lastEntry.getValue());
            position -= rowSize(lastEntry.getValue());
        }
        if (rowMap.isEmpty() && lastEntry != null) {
            // 当前的第一个节点即为大节点
            insertLarge(k, v, newRowSize);
            updateTimeWindowBeforeFlushing();
//            checkAndFlush();
            return num;
        }

        // 当前的rowMap已经调整完毕，在可能发生页connect前updateTimeWindow
        updateTimeWindowBeforeFlushing();
        memPage.unwrap().position(position);

        if (transfer != null && !transfer.isEmpty()) {
            boolean needCreatNewPage = false;
            List<Row> transferToNewPage = new LinkedList<>();
            if (this.rightNum == -1) {
                // 转移到新的一页
                needCreatNewPage = true;
                transferToNewPage.addAll(transfer);
            } else {
                // 转移到后一页
                TimeSortedPage rightPage = vinStorage.getPage(TimeSortedPage.class, this.rightNum);
                for (Row row : transfer) {
                    // transfer是按时间倒序的
                    if (!needCreatNewPage && rightPage.insert(row.getTimestamp(), row) == this.num) {
                        transferToNewPage.add(row);
                        needCreatNewPage = true;
                        continue;
                    }
                    transferToNewPage.add(row);
                }
            }
            if (needCreatNewPage) {
                TimeSortedPage newPage = vinStorage.creatPage(TimeSortedPage.class);
                this.connectRightBeforeFlushingByForce(newPage);
                for (Row row : transferToNewPage) {
                    newPage.insert(row.getTimestamp(), row);
                }
            }
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

    protected void connectRightBeforeFlushingByForce(TimeSortedPage right) {
        TimeSortedPage oldRPage = vinStorage.getPage(TimeSortedPage.class, this.rightNum);
        if (oldRPage != null) {
            oldRPage.leftNum = right.num;
            right.rightNum = oldRPage.num;
        }
        right.leftNum = this.num;
        this.rightNum = right.num;
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

        List<String> columnNameList = vinStorage.columnNameList();
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
