package com.alibaba.lindorm.contest.storage;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DataPage extends AbPage {

    public DataPage(FileChannel fileChannel) {
        super(fileChannel);
        this.isDataPage = true;
    }

    /**
     * 如果当前不是NULL_PAGE，则表示往后还有更多的数据页
     */
    private int nextNum;

    private byte[] data;

}
