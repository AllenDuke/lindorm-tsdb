package com.alibaba.lindorm.contest.elf;

public class ChimpCompressor implements ICompressor {
    private final Chimp chimp;
    public ChimpCompressor(int bufferSize) {
        chimp = new Chimp(bufferSize);
    }
    @Override public void addValue(double v) {
        chimp.addValue(v);
    }

    @Override public int getSize() {
        return chimp.getSize();
    }

    @Override public byte[] getBytes() {
        return chimp.getOut();
    }

    @Override public void close() {
        chimp.close();
    }
}
