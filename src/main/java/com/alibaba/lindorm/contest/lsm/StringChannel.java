package com.alibaba.lindorm.contest.lsm;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.structs.ColumnValue;

import java.io.File;
import java.io.IOException;

public class StringChannel extends ColumnChannel<ColumnValue.StringColumn>{

    public StringChannel(File vinDir, TableSchema.Column column) throws IOException {
        super(vinDir, column);
    }

    @Override
    public void append(ColumnValue.StringColumn stringColumn) throws IOException {
        CommonUtils.writeString(columnOutput, stringColumn.getStringValue());
    }
}
