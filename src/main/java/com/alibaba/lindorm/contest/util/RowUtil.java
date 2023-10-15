package com.alibaba.lindorm.contest.util;

import com.alibaba.lindorm.contest.lsm.TableSchema;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;

import java.nio.ByteBuffer;
import java.util.*;

public class RowUtil {

    public static List<Row> toRowList(TableSchema tableSchema, ByteBuffer buffer) {
        List<Row> rowList = new ArrayList<>();
        while (buffer.hasRemaining()) {
            long time = buffer.getLong();
            Map<String, ColumnValue> columns = new HashMap<>();
            for (int j = 0; j < tableSchema.getColumnList().size(); j++) {
                ColumnValue.ColumnType columnType = tableSchema.getColumnList().get(j).columnType;
                String columnName = tableSchema.getColumnList().get(j).columnName;
                ColumnValue cVal;
                switch (columnType) {
                    case COLUMN_TYPE_INTEGER:
                        int intVal = buffer.getInt();
                        cVal = new ColumnValue.IntegerColumn(intVal);
                        break;
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        double doubleVal = buffer.getDouble();
                        cVal = new ColumnValue.DoubleFloatColumn(doubleVal);
                        break;
                    case COLUMN_TYPE_STRING:
                        int strLen = buffer.getInt();
                        byte[] strBytes = new byte[strLen];
                        buffer.get(strBytes);
                        cVal = new ColumnValue.StringColumn(ByteBuffer.wrap(strBytes));
                        break;
                    default:
                        throw new IllegalStateException("Undefined column type, this is not expected");
                }
                columns.put(columnName, cVal);
            }
            Row row = new Row(null, time, columns);
            rowList.add(row);
        }
        return rowList;
    }


    public static int timeAndColumnSize(Row row) {
        int size = 8;

        for (ColumnValue cVal : row.getColumns().values()) {
            switch (cVal.getColumnType()) {
                case COLUMN_TYPE_STRING:
                    size += 4;
                    size += cVal.getStringValue().remaining();
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

    public static ByteBuffer toByteBuffer(TableSchema tableSchema, Row row) {
        ByteBuffer buffer = ByteBuffer.allocate((timeAndColumnSize(row)));
        buffer.putLong(row.getTimestamp());
        tableSchema.getColumnList().forEach(column -> {
            ColumnValue cVal = row.getColumns().get(column.columnName);
            switch (cVal.getColumnType()) {
                case COLUMN_TYPE_STRING:
                    buffer.putInt(cVal.getStringValue().limit());
                    buffer.put(cVal.getStringValue());
                    break;
                case COLUMN_TYPE_INTEGER:
                    buffer.putInt(cVal.getIntegerValue());
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    buffer.putDouble(cVal.getDoubleFloatValue());
                    break;
                default:
                    throw new IllegalStateException("Invalid column type");
            }
        });
        buffer.flip();
        return buffer;
    }

    public static void toByteBuffer(TableSchema tableSchema, Row row, ByteBuffer buffer) {
        buffer.putLong(row.getTimestamp());
        tableSchema.getColumnList().forEach(column -> {
            ColumnValue cVal = row.getColumns().get(column.columnName);
            switch (cVal.getColumnType()) {
                case COLUMN_TYPE_STRING:
                    buffer.putInt(cVal.getStringValue().limit());
                    buffer.put(cVal.getStringValue());
                    break;
                case COLUMN_TYPE_INTEGER:
                    buffer.putInt(cVal.getIntegerValue());
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    buffer.putDouble(cVal.getDoubleFloatValue());
                    break;
                default:
                    throw new IllegalStateException("Invalid column type");
            }
        });
    }
}
