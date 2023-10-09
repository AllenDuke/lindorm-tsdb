package com.alibaba.lindorm.contest.elf;

public interface ICompressor {
    void addValue(double v);
    int getSize();
    byte[] getBytes();
    void close();
    default String getKey() {
        return getClass().getSimpleName();
    }
}
