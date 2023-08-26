package com.alibaba.lindorm.contest.storage;

public enum PageStat {

    /**
     * 新建
     */
    NEW,

    /**
     * 从文件中恢复字节数据
     */
    RECOVERED,

    /**
     * 恢复页头信息
     */
    RECOVERED_HEAD,

    /**
     * 恢复页结构化信息
     */
    RECOVERED_ALL,

    /**
     * 已刷盘
     */
    FLUSHED,
}
