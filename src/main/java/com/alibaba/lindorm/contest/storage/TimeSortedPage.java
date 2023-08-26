package com.alibaba.lindorm.contest.storage;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class TimeSortedPage extends AbPage {

    public TimeSortedPage(VinStorage vinStorage, BufferPool bufferPool, Integer num) {
        super(vinStorage, bufferPool, num);
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
    public synchronized void recover() throws IOException {
        if (stat != PageStat.FLUSHED) {
            return;
        }
        super.recover();
        recoverHead();
        recoverAll();
    }

    private synchronized void recoverHead() throws IOException {
        if (stat != PageStat.FLUSHED) {
            return;
        }

        super.recover();

        leftNum = dataBuffer.unwrap().getInt();
        minTime = dataBuffer.unwrap().getLong();
        rightNum = dataBuffer.unwrap().getInt();
        maxTime = dataBuffer.unwrap().getLong();
        extNum = dataBuffer.unwrap().getInt();
        if (extNum != -1) {
            extPageList = new ArrayList<>();
        }
        rowCountOrBigRowSize = dataBuffer.unwrap().getInt();

        stat = PageStat.RECOVERED_HEAD;
    }

    private synchronized void recoverAll() throws IOException {
        if (stat != PageStat.RECOVERED_HEAD) {
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

        stat = PageStat.RECOVERED_ALL;
    }

    private void recoverLarge() {
        long timestamp = dataBuffer.unwrap().getLong();

        /**
         * 大->小，rowCountOrBigRowSize使得数据准确。
         * 但当前扩展页只增不减，可能造成浪费
         */

        ByteBuffer allocate = ByteBuffer.allocate(4 + rowCountOrBigRowSize);
        allocate.position(8);
        allocate.put(dataBuffer.unwrap());
        for (ExtPage extPage : extPageList) {
            allocate.put(extPage.getData());
        }
        allocate.flip();
        allocate.position(8);

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

        ByteBuffer allocate = ByteBuffer.allocate(4 + bigRowSize);
        allocate.putInt(bigRowSize);

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

        dataBuffer.unwrap().put(allocate);
        for (ExtPage extPage : extPageList) {
            extPage.putData(allocate);
            extPage.flush();
        }
    }

    private void flushNormal() {
        dataBuffer.unwrap().putInt(rowMap.size());

        for (Row row : rowMap.values()) {
            dataBuffer.unwrap().putLong(row.getTimestamp());
            List<String> columnNameList = vinStorage.columnNameList();
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
    }

    private void recoverNormal() {
        ArrayList<ColumnValue.ColumnType> columnTypeList = vinStorage.columnTypeList();
        ArrayList<String> columnNameList = vinStorage.columnNameList();

        int rowCount = rowCountOrBigRowSize;
        for (int i = 0; i < rowCount; i++) {
            long timestamp = dataBuffer.unwrap().getLong();
            Map<String, ColumnValue> columns = new HashMap<>();
            for (int j = 0; j < columnTypeList.size(); j++) {
                ColumnValue.ColumnType columnType = columnTypeList.get(j);
                String columnName = columnNameList.get(j);
                ColumnValue cVal;
                switch (columnType) {
                    case COLUMN_TYPE_INTEGER:
                        int intVal = dataBuffer.unwrap().getInt();
                        cVal = new ColumnValue.IntegerColumn(intVal);
                        break;
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        double doubleVal = dataBuffer.unwrap().getDouble();
                        cVal = new ColumnValue.DoubleFloatColumn(doubleVal);
                        break;
                    case COLUMN_TYPE_STRING:
                        int strLen = dataBuffer.unwrap().getInt();
                        byte[] strBytes = new byte[strLen];
                        dataBuffer.unwrap().get(strBytes);
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
    public synchronized void flush() throws IOException {
        if (stat == PageStat.FLUSHED) {
            return;
        }

        if (rowMap.isEmpty()) {
            throw new IllegalStateException("刷盘异常，页数据为空");
        }

        dataBuffer.unwrap().position(0);
        dataBuffer.unwrap().putInt(leftNum);
        dataBuffer.unwrap().putLong(minTime);
        dataBuffer.unwrap().putInt(rightNum);
        dataBuffer.unwrap().putLong(maxTime);

        if (extNum == -1) {
            // 用extNum判断才是正确，因为当前页可能因为更新而大变小
            dataBuffer.unwrap().putInt(-1);
            flushNormal();
        } else {
            dataBuffer.unwrap().putInt(extPageList.get(0).num);
            flushLarge();
        }

        super.flush();

        // 释放内存
        rowMap.clear();
        extPageList = null;

        stat = PageStat.FLUSHED;
    }

    private void firstInsert(long k, Row v) {
        minTime = k;
        maxTime = k;
        leftNum = -1;
        rightNum = -1;

        extNum = -1;
//        rowCountOrBigRowSize = rowSize(v);
        dataBuffer.unwrap().position(4 + 8 + 4 + 8 + 4 + 4);
    }

    /**
     * 插入大数据
     *
     * @param k
     * @param v
     */
    private void insertLarge(long k, Row v, int newRowSize) {
//        rowCountOrBigRowSize = vTotalSize;

        // 4字节保存行大小
        int totalSize = newRowSize + 4;

        // 填满当前页
        totalSize -= dataBuffer.unwrap().remaining();
        dataBuffer.unwrap().position(dataBuffer.unwrap().limit());

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
        }

        extNum = extPageList.get(0).num;

        rowMap.put(k, v);
    }

    public synchronized WindowSearchResult search(WindowSearchRequest request) throws IOException {
        recoverHead();
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

        Collection<Row> rows = rowMap.tailMap(leftTime).headMap(rightTime).values();
        result.setRowList(new LinkedList<>(rows));
        return result;
    }

    /**
     * k v会立即开始读
     *
     * @param k
     * @param v
     * @return 如果可插入当前节点，那么返回当前页号，否则返回下一个尝试插入的页号
     */
    public synchronized int insert(long k, Row v) throws IOException {
        recoverHead();

        // 这里不用rowMap是否empty来判断页是否为空，因为recoverHead不会去构建rowMap
        if ((minTime != -1 && maxTime != -1) && (k < minTime || k > maxTime)) {
            // 不能插入当前节点
            if (k < minTime) {
                return leftNum;
            } else {
                return rightNum;
            }
        }

        // 准备插入当前节点

        recoverAll();

        if (rowMap.isEmpty()) {
            firstInsert(k, v);
        }

        // 插入map
        Row oldV = rowMap.put(k, v);

        // 准备调整指针
        int position = dataBuffer.unwrap().position();

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

        // 准备追加行数据
        int newRowSize = rowSize(v);
        position += newRowSize;

        /**
         * 检查容量
         * 如果当前节点不足以完整插入该行记录，那么当前节点分裂，从当前页的尾部节点开始拷贝节点到新的一页
         */
        List<Row> transfer = null;
        Map.Entry<Long, Row> lastEntry = null;
        while (position > dataBuffer.unwrap().limit()) {
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
            return num;
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

        dataBuffer.unwrap().position(position);

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
