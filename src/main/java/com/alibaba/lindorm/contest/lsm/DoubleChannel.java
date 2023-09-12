package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.structs.ColumnValue;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class DoubleChannel extends ColumnChannel<ColumnValue.DoubleFloatColumn>{

    public DoubleChannel(File vinDir, TableSchema.Column column) throws IOException {
        super(vinDir, column);
    }

    @Override
    public void append(ColumnValue.DoubleFloatColumn doubleFloatColumn) throws IOException {
        CommonUtils.writeDouble(columnOutput, doubleFloatColumn.getDoubleFloatValue());
    }

    @Override
    public List<ColumnItem<ColumnValue.DoubleFloatColumn>> range(List<TimeItem> timeItemList) throws IOException {
        return null;
    }
}
