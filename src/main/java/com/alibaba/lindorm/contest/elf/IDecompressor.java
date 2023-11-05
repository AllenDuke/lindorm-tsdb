package com.alibaba.lindorm.contest.elf;

import java.util.List;

public interface IDecompressor {
    List<Double> decompress();

    List<Double> decompress(long batchNumBegin, List<Long> batchNumList);
}
