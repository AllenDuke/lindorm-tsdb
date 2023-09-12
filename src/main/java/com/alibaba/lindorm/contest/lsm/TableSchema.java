package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.structs.ColumnValue;

import java.util.ArrayList;
import java.util.List;

public class TableSchema {

    public static class Column {

        String columnName;

        ColumnValue.ColumnType columnType;
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
