package com.alibaba.lindorm.contest.elf;

public class ChimpNCompressor implements ICompressor {
    private final ChimpN chimpN;

    public ChimpNCompressor(int bufferSize, int previousValues) {
        chimpN = new ChimpN(bufferSize, previousValues);
    }

    @Override
    public void addValue(double v) {
        chimpN.addValue(v);
    }

    @Override
    public int getSize() {
        return chimpN.getSize();
    }

    @Override
    public byte[] getBytes() {
        return chimpN.getOut();
    }

    @Override
    public void close() {
        chimpN.close();
    }
}
