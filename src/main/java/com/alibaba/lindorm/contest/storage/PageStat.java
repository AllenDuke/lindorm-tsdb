package com.alibaba.lindorm.contest.storage;

public enum PageStat {

    /**
     * 页key状态
     */
    KEY,

    /**
     * 使用中，不需要recover
     */
    USING,

    /**
     * 从文件中恢复字节数据
     */
    RECOVERED,

    /**
     * 恢复页头信息
     */
    RECOVERED_HEAD,

    /**
     * 已刷盘
     */
    FLUSHED,
}
