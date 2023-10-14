package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.structs.ColumnValue;

import java.util.ArrayList;
import java.util.List;

public class TableSchema {

    public static class Column {
        public String columnName;

        public ColumnValue.ColumnType columnType;

        int indexSize;

        @Override
        public int hashCode() {
            return columnName.hashCode() + columnType.ordinal();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Column)) {
                return false;
            }
            return ((Column) obj).columnName.equals(this.columnName) && ((Column) obj).columnType.equals(this.columnType);
        }
    }

    private final List<Column> columnList;

    public TableSchema(List<String> columnNameList, List<ColumnValue.ColumnType> columnTypeList) {
        columnList = new ArrayList<>(columnNameList.size());
        for (int i = 0; i < columnNameList.size(); i++) {
            Column column = new Column();
            column.columnName = columnNameList.get(i);
            column.columnType = columnTypeList.get(i);
            columnList.add(column);
        }
    }

    public List<Column> getColumnList() {
        return columnList;
    }
}
