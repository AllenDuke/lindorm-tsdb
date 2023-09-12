package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SsTable {

    private final OutputStream outputStream;

    private long minK = 0;

    private long maxK = 0;

    private int itemCnt = 0;

    private final TableSchema tableSchema;

    private Map<String, ColumnValue> lastColumns;

    private Row lastRow;

    public SsTable(OutputStream outputStream, TableSchema tableSchema) {
        this.outputStream = outputStream;
        this.tableSchema = tableSchema;
    }

    private void updateLastRow(Row row) {
        Map<String, ColumnValue> columns = row.getColumns();
        if (lastColumns == null) {
            lastColumns = new HashMap<>(columns.size());
        } else {
            lastColumns.clear();
        }
        columns.forEach((k, v) -> {
            if (v.getColumnType() != ColumnValue.ColumnType.COLUMN_TYPE_STRING) {
                lastColumns.put(k, v);
            }
        });
        lastRow = new Row(row.getVin(), row.getTimestamp(), lastColumns);
    }

    private void appendRowToFile(List<String> columnsName, Row row) {
        int columnsNum = columnsName.size();
        if (row.getColumns().size() != columnsNum) {
            System.err.println("Cannot write a non-complete row with columns' num: [" + row.getColumns().size() + "]. ");
            System.err.println("There are [" + columnsNum + "] rows in total");
            throw new RuntimeException();
        }

        try {
            // 整数型存delta
            CommonUtils.writeInt(outputStream, (int) (row.getTimestamp() - lastRow.getTimestamp()));
            for (int i = 0; i < columnsNum; ++i) {
                String cName = columnsName.get(i);
                ColumnValue cVal = row.getColumns().get(cName);
                switch (cVal.getColumnType()) {
                    case COLUMN_TYPE_STRING:
                        CommonUtils.writeString(outputStream, cVal.getStringValue());
                        break;
                    case COLUMN_TYPE_INTEGER:
                        CommonUtils.writeShort(outputStream, (short) (cVal.getIntegerValue() - lastRow.getColumns().get(cName).getIntegerValue()));
                        break;
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        CommonUtils.writeDouble(outputStream, cVal.getDoubleFloatValue());
                        break;
                    default:
                        throw new IllegalStateException("Invalid column type");
                }
            }
//            fout.flush();
        } catch (IOException e) {
            System.err.println("Error writing row to file");
            throw new RuntimeException(e);
        }
    }

    private void appendFirstRowToFile(List<String> columnsName, Row row) {
        int columnsNum = columnsName.size();
        if (row.getColumns().size() != columnsNum) {
            System.err.println("Cannot write a non-complete row with columns' num: [" + row.getColumns().size() + "]. ");
            System.err.println("There are [" + columnsNum + "] rows in total");
            throw new RuntimeException();
        }

        try {
            CommonUtils.writeLong(outputStream, row.getTimestamp());
            for (int i = 0; i < columnsNum; ++i) {
                String cName = columnsName.get(i);
                ColumnValue cVal = row.getColumns().get(cName);
                switch (cVal.getColumnType()) {
                    case COLUMN_TYPE_STRING:
                        CommonUtils.writeString(outputStream, cVal.getStringValue());
                        break;
                    case COLUMN_TYPE_INTEGER:
                        CommonUtils.writeInt(outputStream, cVal.getIntegerValue());
                        break;
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        CommonUtils.writeDouble(outputStream, cVal.getDoubleFloatValue());
                        break;
                    default:
                        throw new IllegalStateException("Invalid column type");
                }
            }
//            fout.flush();
        } catch (IOException e) {
            System.err.println("Error writing row to file");
            throw new RuntimeException(e);
        }
    }

    public void put(long k, Row row) {
        maxK = Math.max(k, maxK);
        minK = Math.min(k, minK);

        if (lastRow == null) {
            appendFirstRowToFile(columnsName, row);
        } else {
            appendRowToFile(columnsName, row);
            updateLastRow(row);
        }

        itemCnt++;
    }

    public int getItemCnt() {
        return itemCnt;
    }

    public void flush() throws IOException {
        CommonUtils.writeLong(outputStream, minK);
        CommonUtils.writeLong(outputStream, maxK);
        outputStream.flush();
        outputStream.close();
    }
}
